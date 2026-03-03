import asyncio
import os
import shutil
import urllib.parse
import random
import time
import hashlib
import json
from pathlib import Path
import httpx

from config import (VERSION, UPLOAD_CHUNK_SIZE, UPLOAD_START_RETRIES, 
                    UPLOAD_CHUNK_RETRIES, UPLOAD_FINISH_RETRIES, 
                    RETRIABLE_STATUS_CODES, ARIA_PROGRESS_REGEX)
from utils import secure_filename, _retry_sleep

class WorkerEngine:
    def __init__(self, config, ui_callbacks):
        self.cfg = config
        self.ui = ui_callbacks 
        self.stop_event = asyncio.Event()
        self.headers = {"Authorization": f"Bearer {config['token']}", "X-Minerva-Worker-Version": VERSION}
        self.aria2c_path = shutil.which("aria2c")

    async def report_job(self, file_id, status, bytes_downloaded=None, error=None):
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                await client.post(
                    f"{self.cfg['api_server']}/api/jobs/report",
                    headers=self.headers,
                    json={"file_id": file_id, "status": status, "bytes_downloaded": bytes_downloaded, "error": error}
                )
            except httpx.RequestError:
                pass

    async def download_file(self, url, dest, known_size, ui_job_id):
        dest.parent.mkdir(parents=True, exist_ok=True)
        if self.aria2c_path:
            proc = await asyncio.create_subprocess_exec(
                self.aria2c_path, 
                f"--max-connection-per-server={self.cfg['aria_conns']}",
                f"--split={self.cfg['aria_conns']}", 
                "--min-split-size=1M",
                "--dir", str(dest.parent), 
                "--out", dest.name,
                "--console-log-level=notice", 
                "--summary-interval=1", 
                "--continue=true", 
                "--disk-cache=16384M",               # ADDED
                "--optimize-concurrent-downloads=true", # ADDED
                url,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )
            while True:
                line = await proc.stdout.readline()
                if not line: break
                
                line_str = line.decode('utf-8', errors='replace')
                match = ARIA_PROGRESS_REGEX.search(line_str)
                if match:
                    self.ui['progress'](
                        ui_job_id, 
                        int(match.group(3)) / 100.0, 
                        f"{match.group(4).strip()}/s", 
                        "Downloading", 
                        f"{match.group(1).strip()} / {match.group(2).strip()}",
                        f"ETA: {match.group(5).strip() if match.group(5) else '...'}"
                    )
            await proc.wait()
            if proc.returncode != 0: raise RuntimeError(f"aria2c error {proc.returncode}")
        return dest

    async def upload_file(self, file_id, path: Path, ui_job_id):
        timeout = httpx.Timeout(connect=15, read=120, write=120, pool=30)
        async with httpx.AsyncClient(timeout=timeout) as client:
            session_id = None
            self.ui['log'](f"[{path.name}] Requesting upload session from {self.cfg['upload_server']}...")
            for attempt in range(1, UPLOAD_START_RETRIES + 1):
                try:
                    resp = await client.post(f"{self.cfg['upload_server']}/api/upload/{file_id}/start", headers=self.headers)
                    if resp.status_code in RETRIABLE_STATUS_CODES:
                        self.ui['log'](f"[{path.name}] Upload server busy (HTTP {resp.status_code}). Retrying {attempt}/{UPLOAD_START_RETRIES}...")
                        await asyncio.sleep(_retry_sleep(attempt))
                        continue
                    resp.raise_for_status()
                    session_id = resp.json()["session_id"]
                    break
                except httpx.HTTPError as e:
                    self.ui['log'](f"[{path.name}] Upload connection error: {e}")
                    await asyncio.sleep(_retry_sleep(attempt))
            
            if not session_id: raise RuntimeError("Failed to connect to Upload Server after retries.")

            file_size = path.stat().st_size
            sent = 0
            hasher = hashlib.sha256()
            start_time = time.time()
            
            self.ui['log'](f"[{path.name}] Upload session acquired. Sending data...")
            loop = asyncio.get_running_loop()
            
            with open(path, "rb") as f:
                while True:
                    data = await loop.run_in_executor(None, f.read, UPLOAD_CHUNK_SIZE)
                    if not data: break
                    
                    hasher.update(data)
                    for attempt in range(1, UPLOAD_CHUNK_RETRIES + 1):
                        try:
                            resp = await client.post(
                                f"{self.cfg['upload_server']}/api/upload/{file_id}/chunk",
                                params={"session_id": session_id},
                                headers={**self.headers, "Content-Type": "application/octet-stream"},
                                content=data,
                            )
                            if resp.status_code in RETRIABLE_STATUS_CODES:
                                await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                                continue
                            resp.raise_for_status()
                            break
                        except httpx.HTTPError:
                            await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                            
                    sent += len(data)
                    elapsed = time.time() - start_time
                    speed_bps = sent / elapsed if elapsed > 0 else 0
                    speed_mbs = speed_bps / (1024 * 1024)
                    
                    eta_sec = (file_size - sent) / speed_bps if speed_bps > 0 else 0
                    eta_m, eta_s = divmod(int(eta_sec), 60)
                    
                    pct = sent / file_size if file_size > 0 else 1.0
                    self.ui['progress'](
                        ui_job_id, pct, f"↑ {speed_mbs:.1f} MB/s", "Uploading", 
                        f"{sent/1024/1024:.1f}MB / {file_size/1024/1024:.1f}MB", f"ETA: {eta_m}m {eta_s}s"
                    )

            self.ui['log'](f"[{path.name}] Upload stream finished. Verifying checksum...")
            expected_sha256 = hasher.hexdigest()
            for attempt in range(1, UPLOAD_FINISH_RETRIES + 1):
                try:
                    resp = await client.post(
                        f"{self.cfg['upload_server']}/api/upload/{file_id}/finish",
                        params={"session_id": session_id, "expected_sha256": expected_sha256},
                        headers=self.headers,
                    )
                    if resp.status_code in RETRIABLE_STATUS_CODES:
                        await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                        continue
                    resp.raise_for_status()
                    break
                except httpx.HTTPError:
                    await asyncio.sleep(_retry_sleep(attempt, cap=20.0))

    async def process_job(self, job):
        file_id, url, raw_dest_path = job["file_id"], job["url"], job["dest_path"]
        
        parsed = urllib.parse.urlparse(url)
        host = "".join("_" if ch in '<>:"/\\|?*' else ch for ch in (parsed.netloc or "unknown")).strip()
        clean_filename = secure_filename(raw_dest_path)
        
        local_path = Path(self.cfg['temp_dir']).resolve() / host / clean_filename
        ui_job_id = str(file_id) 
        
        job_cache_file = local_path.with_name(local_path.name + ".job.json")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(job_cache_file, 'w') as f:
            json.dump(job, f)
            
        self.ui['log'](f"Starting job: {clean_filename}")
        self.ui['new_job'](ui_job_id, clean_filename)

        try:
            await asyncio.sleep(random.uniform(0.5, 2.0))
            await self.download_file(url, local_path, job.get('size', 0), ui_job_id)
            
            file_size = local_path.stat().st_size if local_path.exists() else 0
            
            self.ui['progress'](ui_job_id, 0.0, "↑ 0.0 MB/s", "Uploading", "Connecting to server...", "ETA: --")
            await self.upload_file(file_id, local_path, ui_job_id)
            
            self.ui['progress'](ui_job_id, 1.0, "Complete", "Complete", "Upload Verified", "ETA: Done")
            self.ui['log'](f"Successfully processed: {clean_filename}")
            await self.report_job(file_id, "completed", bytes_downloaded=file_size)

        except Exception as e:
            safe_error = str(e).replace(self.cfg['token'], "[REDACTED]") if self.cfg['token'] else str(e)
            self.ui['log'](f"Error on {clean_filename}: {safe_error}")
            self.ui['progress'](ui_job_id, 0.0, "Failed", "Failed", safe_error[:40], "ETA: Error")
            await self.report_job(file_id, "failed", error=safe_error)
        finally:
            job_cache_file.unlink(missing_ok=True)
            aria2_file = local_path.with_name(local_path.name + ".aria2")
            aria2_file.unlink(missing_ok=True)
            if not self.cfg['keep_files']:
                local_path.unlink(missing_ok=True)

    async def run_loop(self):
        queue = asyncio.Queue(maxsize=self.cfg['concurrency'] * 2)
        seen_ids = set()

        recovered_count = 0
        temp_dir_path = Path(self.cfg['temp_dir'])
        if temp_dir_path.exists():
            for job_file in temp_dir_path.rglob('*.job.json'):
                try:
                    with open(job_file, 'r') as f:
                        cached_job = json.load(f)
                    if cached_job["file_id"] not in seen_ids:
                        seen_ids.add(cached_job["file_id"])
                        await queue.put(cached_job)
                        recovered_count += 1
                except Exception:
                    pass
        if recovered_count > 0:
            self.ui['log'](f"Loaded {recovered_count} interrupted jobs from local storage. Resuming...")

        async def producer():
            async with httpx.AsyncClient(timeout=10.0) as client:
                while not self.stop_event.is_set():
                    if queue.qsize() >= self.cfg['concurrency']:
                        await asyncio.sleep(1)
                        continue
                    try:
                        resp = await client.get(
                            f"{self.cfg['api_server']}/api/jobs",
                            params={"count": self.cfg['batch_size']},
                            headers=self.headers
                        )
                        if resp.status_code == 200:
                            for job in resp.json().get("jobs", []):
                                if job["file_id"] not in seen_ids:
                                    seen_ids.add(job["file_id"])
                                    await queue.put(job)
                        else:
                            await asyncio.sleep(10)
                    except httpx.RequestError:
                        await asyncio.sleep(10)

        async def worker():
            while not self.stop_event.is_set() or not queue.empty():
                try:
                    job = await queue.get()
                    await self.process_job(job)
                    queue.task_done()
                except asyncio.CancelledError:
                    break

        self.ui['log'](f"Worker engine started. CPU Cores available: {os.cpu_count()}")
        prod_task = asyncio.create_task(producer())
        work_tasks = [asyncio.create_task(worker()) for _ in range(self.cfg['concurrency'])]
        
        await self.stop_event.wait()
        prod_task.cancel()
        for t in work_tasks: t.cancel()
        self.ui['log']("Worker engine stopped.")