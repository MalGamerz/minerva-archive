from __future__ import annotations

import asyncio
import collections
import hashlib
import json
import os
import random
import shutil
import sys
import time
import urllib.parse
from pathlib import Path

import httpx

from config import (
    VERSION, UPLOAD_CHUNK_SIZE,
    UPLOAD_START_RETRIES, UPLOAD_CHUNK_RETRIES, UPLOAD_FINISH_RETRIES,
    RETRIABLE_STATUS_CODES, ARIA_PROGRESS_REGEX, ARIA_BASE_ARGS,
    PRODUCER_POLL_INTERVAL, PRODUCER_BACKOFF_INTERVAL,
)
from utils import secure_filename, _retry_sleep


class WorkerEngine:

    def __init__(self, config: dict, ui_callbacks: dict):
        self.cfg = config
        self.ui = ui_callbacks
        self.stop_event = asyncio.Event()
        self.headers = {
            "Authorization": f"Bearer {config['token']}",
            "X-Minerva-Worker-Version": VERSION,
        }
        self.aria2c_path = shutil.which("aria2c")

        self._http_limits = httpx.Limits(
            max_connections=64,
            max_keepalive_connections=32,
            keepalive_expiry=30,
        )

    def _redact(self, text: str) -> str:
        token = self.cfg.get("token") or ""
        return text.replace(token, "[REDACTED]") if token else text

    async def report_job(
        self, file_id, status: str, *, bytes_downloaded: int | None = None, error: str | None = None,
    ) -> None:
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                await client.post(
                    f"{self.cfg['api_server']}/api/jobs/report",
                    headers=self.headers,
                    json={
                        "file_id": file_id,
                        "status": status,
                        "bytes_downloaded": bytes_downloaded,
                        "error": error,
                    },
                )
            except httpx.RequestError:
                pass

    async def download_file(
        self, url: str, dest: Path, known_size: int, ui_job_id: str
    ) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        if self.aria2c_path:
            await self._download_aria2c(url, dest, ui_job_id)
        else:
            self.ui["log"](f"[{dest.name}] aria2c not found — using HTTPX fallback.")
            await self._download_httpx(url, dest, known_size, ui_job_id)
        return dest

    async def _download_aria2c(self, url: str, dest: Path, ui_job_id: str) -> None:
        conns = self.cfg["aria_conns"]
        args = [
            self.aria2c_path,
            f"--max-connection-per-server={conns}",
            f"--split={conns}",
            "--dir", str(dest.parent),
            "--out", dest.name,
            *ARIA_BASE_ARGS,
            url,
        ]

        kwargs: dict = {
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.STDOUT,
        }
        if sys.platform == "win32":
            kwargs["creationflags"] = 0x08000000 

        proc = await asyncio.create_subprocess_exec(*args, **kwargs)

        try:
            async for raw in proc.stdout:
                line = raw.decode("utf-8", errors="replace")
                m = ARIA_PROGRESS_REGEX.search(line)
                if m:
                    self.ui["progress"](
                        ui_job_id,
                        int(m.group(3)) / 100.0,
                        f"{m.group(4).strip()}/s",
                        "Downloading",
                        f"{m.group(1).strip()} / {m.group(2).strip()}",
                        f"ETA: {m.group(5).strip() if m.group(5) else '…'}",
                    )

            await proc.wait()
            if proc.returncode != 0:
                raise RuntimeError(f"aria2c exited with code {proc.returncode}")
                
        except asyncio.CancelledError:
            # THIS IS THE FIX: Assassinate the zombie process when "Stop" is clicked
            if proc.returncode is None:
                try:
                    proc.kill()
                except OSError:
                    pass
            raise

    async def _download_httpx(
        self, url: str, dest: Path, known_size: int, ui_job_id: str
    ) -> None:
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=30.0, limits=self._http_limits,
        ) as client:
            try:
                head = await client.head(url)
                total = int(head.headers.get("content-length", known_size or 0))
                accepts_ranges = head.headers.get("accept-ranges", "none").lower() == "bytes"
            except Exception:
                total = known_size or 0
                accepts_ranges = False

            n_chunks = self.cfg.get("aria_conns", 4) if accepts_ranges and total > 0 else 1

            if n_chunks > 1:
                await self._download_httpx_parallel(client, url, dest, total, n_chunks, ui_job_id)
            else:
                await self._download_httpx_stream(client, url, dest, total, ui_job_id)

    async def _download_httpx_parallel(
        self, client: httpx.AsyncClient, url: str, dest: Path, total: int, n: int, ui_job_id: str,
    ) -> None:
        chunk_size = total // n
        ranges = [(i * chunk_size, (i + 1) * chunk_size - 1 if i < n - 1 else total - 1) for i in range(n)]

        tmp_parts = [dest.with_suffix(f".part{i}") for i in range(n)]
        downloaded_bytes = [0] * n
        start_time = time.monotonic()

        async def fetch_part(idx: int, start: int, end: int, out: Path) -> None:
            headers = {"Range": f"bytes={start}-{end}"}
            for attempt in range(10):
                try:
                    async with client.stream("GET", url, headers=headers) as r:
                        r.raise_for_status()
                        with open(out, "wb") as fh:
                            async for chunk in r.aiter_bytes(65_536):
                                fh.write(chunk)
                                downloaded_bytes[idx] += len(chunk)
                    return
                except Exception:
                    await asyncio.sleep(_retry_sleep(attempt + 1))
            raise RuntimeError(f"Part {idx} failed after retries")

        tasks = [asyncio.create_task(fetch_part(i, s, e, tmp_parts[i])) for i, (s, e) in enumerate(ranges)]

        async def _report():
            while not all(t.done() for t in tasks):
                done = sum(downloaded_bytes)
                elapsed = time.monotonic() - start_time
                speed = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / speed if speed > 0 else 0
                pct = done / total if total > 0 else 0.0
                em, es = divmod(int(eta), 60)
                self.ui["progress"](
                    ui_job_id, pct, f"{speed / 1_048_576:.1f} MB/s", "Downloading",
                    f"{done // 1_048_576} MB / {total // 1_048_576} MB", f"ETA: {em}m {es}s",
                )
                await asyncio.sleep(1.0)

        reporter = asyncio.create_task(_report())
        try:
            await asyncio.gather(*tasks)
        finally:
            reporter.cancel()

        with open(dest, "wb") as out:
            for part in tmp_parts:
                with open(part, "rb") as inp:
                    shutil.copyfileobj(inp, out)
                part.unlink(missing_ok=True)

        self.ui["progress"](
            ui_job_id, 1.0, "Done", "Downloading",
            f"{total // 1_048_576} MB / {total // 1_048_576} MB", "ETA: 0s",
        )

    async def _download_httpx_stream(
        self, client: httpx.AsyncClient, url: str, dest: Path, total: int, ui_job_id: str,
    ) -> None:
        downloaded = 0
        start_time = time.monotonic()
        last_update = 0
        async with client.stream("GET", url) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", total or 0))
            with open(dest, "wb") as fh:
                async for chunk in r.aiter_bytes(65_536):
                    fh.write(chunk)
                    downloaded += len(chunk)
                    now = time.monotonic()
                    if now - last_update > 0.5 or downloaded == total:
                        last_update = now
                        elapsed = now - start_time
                        speed = downloaded / elapsed if elapsed > 0 else 0
                        eta = (total - downloaded) / speed if speed > 0 else 0
                        em, es = divmod(int(eta), 60)
                        self.ui["progress"](
                            ui_job_id, downloaded / total if total else 0.0,
                            f"{speed / 1_048_576:.1f} MB/s", "Downloading",
                            f"{downloaded // 1_048_576} MB / {total // 1_048_576} MB", f"ETA: {em}m {es}s",
                        )

    async def upload_file(self, file_id, path: Path, ui_job_id: str) -> None:
        timeout = httpx.Timeout(connect=15, read=120, write=120, pool=30)
        async with httpx.AsyncClient(timeout=timeout, limits=self._http_limits) as client:
            session_id = await self._upload_start(client, file_id, path)
            sha256 = await self._upload_chunks(client, file_id, path, session_id, ui_job_id)
            await self._upload_finish(client, file_id, path, session_id, sha256)

    async def _upload_start(self, client: httpx.AsyncClient, file_id, path: Path) -> str:
        self.ui["log"](f"[{path.name}] Requesting upload session…")
        for attempt in range(1, UPLOAD_START_RETRIES + 1):
            try:
                resp = await client.post(
                    f"{self.cfg['upload_server']}/api/upload/{file_id}/start", headers=self.headers,
                )
                if resp.status_code == 409: raise FileExistsError("409_CONFLICT")
                if resp.status_code in RETRIABLE_STATUS_CODES:
                    await asyncio.sleep(_retry_sleep(attempt))
                    continue
                resp.raise_for_status()
                return resp.json()["session_id"]
            except FileExistsError: raise
            except httpx.HTTPError as exc:
                self.ui["log"](f"[{path.name}] Upload start error: {exc}")
                await asyncio.sleep(_retry_sleep(attempt))
        raise RuntimeError("Failed to obtain upload session after retries.")

    async def _upload_chunks(self, client: httpx.AsyncClient, file_id, path: Path, session_id: str, ui_job_id: str) -> str:
        file_size = path.stat().st_size
        sent = 0
        hasher = hashlib.sha256()
        start_time = time.monotonic()
        loop = asyncio.get_running_loop()

        self.ui["log"](f"[{path.name}] Upload session acquired. Sending data…")

        with open(path, "rb") as fh:
            while True:
                data = await loop.run_in_executor(None, fh.read, UPLOAD_CHUNK_SIZE)
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
                        if resp.status_code == 409: raise FileExistsError("409_CONFLICT")
                        if resp.status_code in RETRIABLE_STATUS_CODES:
                            await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                            continue
                        resp.raise_for_status()
                        break
                    except FileExistsError: raise
                    except httpx.HTTPError:
                        if attempt == UPLOAD_CHUNK_RETRIES:
                            raise RuntimeError(f"Chunk upload failed after {UPLOAD_CHUNK_RETRIES} attempts.")
                        await asyncio.sleep(_retry_sleep(attempt, cap=20.0))

                sent += len(data)
                elapsed = time.monotonic() - start_time
                speed = sent / elapsed if elapsed > 0 else 0
                eta = (file_size - sent) / speed if speed > 0 else 0
                em, es = divmod(int(eta), 60)
                self.ui["progress"](
                    ui_job_id, sent / file_size if file_size else 1.0, f"↑ {speed / 1_048_576:.1f} MB/s", "Uploading",
                    f"{sent / 1_048_576:.1f} MB / {file_size / 1_048_576:.1f} MB", f"ETA: {em}m {es}s",
                )
        return hasher.hexdigest()

    async def _upload_finish(self, client: httpx.AsyncClient, file_id, path: Path, session_id: str, expected_sha256: str) -> None:
        self.ui["log"](f"[{path.name}] Verifying checksum…")
        for attempt in range(1, UPLOAD_FINISH_RETRIES + 1):
            try:
                resp = await client.post(
                    f"{self.cfg['upload_server']}/api/upload/{file_id}/finish",
                    params={"session_id": session_id, "expected_sha256": expected_sha256},
                    headers=self.headers,
                )
                if resp.status_code == 409: raise FileExistsError("409_CONFLICT")
                if resp.status_code in RETRIABLE_STATUS_CODES:
                    await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                    continue
                resp.raise_for_status()
                return
            except FileExistsError: raise
            except httpx.HTTPError:
                if attempt == UPLOAD_FINISH_RETRIES:
                    raise RuntimeError("Failed to finalise upload after retries.")
                await asyncio.sleep(_retry_sleep(attempt, cap=20.0))

    async def process_job(self, job: dict) -> None:
        file_id = job["file_id"]
        url = job["url"]
        raw_dest = job["dest_path"]

        parsed = urllib.parse.urlparse(url)
        host = "".join("_" if ch in '<>:"/\\|?*' else ch for ch in (parsed.netloc or "unknown")).strip()
        clean_name = secure_filename(raw_dest)
        local_path = Path(self.cfg["temp_dir"]).resolve() / host / clean_name
        ui_job_id = str(file_id)

        local_path.parent.mkdir(parents=True, exist_ok=True)
        job_cache = local_path.with_name(local_path.name + ".job.json")
        job_cache.write_text(json.dumps(job))

        self.ui["log"](f"Starting job: {clean_name}")
        self.ui["new_job"](ui_job_id, clean_name)

        try:
            await asyncio.sleep(random.uniform(0.5, 2.0))
            await self.download_file(url, local_path, job.get("size", 0), ui_job_id)
            file_size = local_path.stat().st_size if local_path.exists() else 0

            self.ui["progress"](ui_job_id, 0.0, "↑ 0.0 MB/s", "Uploading", "Connecting…", "ETA: --")
            await self.upload_file(file_id, local_path, ui_job_id)

            self.ui["progress"](ui_job_id, 1.0, "Complete", "Complete", "Upload Verified", "ETA: Done")
            self.ui["log"](f"Done: {clean_name}")
            await self.report_job(file_id, "completed", bytes_downloaded=file_size)

        except asyncio.CancelledError:
            raise
        except FileExistsError:
            self.ui["log"](f"[{clean_name}] Skipped: already archived (409).")
            self.ui["progress"](ui_job_id, 1.0, "Skipped", "Skipped", "Already Archived", "ETA: Done")
            await self.report_job(file_id, "failed", error="409 Conflict")
        except Exception as exc:
            safe_err = self._redact(str(exc))
            self.ui["log"](f"Error on {clean_name}: {safe_err}")
            self.ui["progress"](ui_job_id, 0.0, "Failed", "Failed", safe_err[:40], "ETA: Error")
            await self.report_job(file_id, "failed", error=safe_err)
        finally:
            # THIS IS FIX #2: Prevents Windows File Lock Permission errors from masking the shutdown signal
            try:
                job_cache.unlink(missing_ok=True)
                local_path.with_name(local_path.name + ".aria2").unlink(missing_ok=True)
                if not self.cfg["keep_files"]:
                    local_path.unlink(missing_ok=True)
            except Exception:
                pass

    async def run_loop(self) -> None:
        queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=self.cfg["concurrency"] * 2)
        seen_ids: collections.deque = collections.deque(maxlen=10_000)

        workers = [asyncio.create_task(self._worker(queue)) for _ in range(self.cfg["concurrency"])]
        await self._recover_interrupted_jobs(queue, seen_ids)

        self.ui["log"](f"Worker started. Concurrency={self.cfg['concurrency']} aria2c={'yes' if self.aria2c_path else 'no'} CPUs={os.cpu_count()}")
        prod = asyncio.create_task(self._producer(queue, seen_ids))

        await self.stop_event.wait()

        prod.cancel()
        for w in workers:
            w.cancel()
        await asyncio.gather(prod, *workers, return_exceptions=True)
        self.ui["log"]("Worker stopped cleanly.")

    async def _recover_interrupted_jobs(self, queue: asyncio.Queue, seen_ids: collections.deque) -> None:
        tmp = Path(self.cfg["temp_dir"])
        if not tmp.exists(): return
        recovered = 0
        for jf in tmp.rglob("*.job.json"):
            try:
                cached = json.loads(jf.read_text())
                if cached["file_id"] not in seen_ids:
                    seen_ids.append(cached["file_id"])
                    await queue.put(cached)
                    recovered += 1
            except Exception:
                pass
        if recovered: self.ui["log"](f"Recovered {recovered} interrupted job(s). Resuming…")

    async def _producer(self, queue: asyncio.Queue, seen_ids: collections.deque) -> None:
        async with httpx.AsyncClient(timeout=10.0, limits=self._http_limits) as client:
            while not self.stop_event.is_set():
                if queue.qsize() >= self.cfg["concurrency"]:
                    await asyncio.sleep(PRODUCER_POLL_INTERVAL)
                    continue
                try:
                    resp = await client.get(
                        f"{self.cfg['api_server']}/api/jobs",
                        params={"count": self.cfg["batch_size"]}, headers=self.headers,
                    )
                    if resp.status_code == 200:
                        jobs = resp.json().get("jobs", [])
                        for job in jobs:
                            if job["file_id"] not in seen_ids:
                                seen_ids.append(job["file_id"])
                                await queue.put(job)
                        if not jobs: await asyncio.sleep(PRODUCER_BACKOFF_INTERVAL)
                    else: await asyncio.sleep(PRODUCER_BACKOFF_INTERVAL)
                except httpx.RequestError:
                    await asyncio.sleep(PRODUCER_BACKOFF_INTERVAL)

    async def _worker(self, queue: asyncio.Queue) -> None:
        # THIS IS FIX #3: Instantly drop all queued jobs the moment "Stop" is clicked
        while not self.stop_event.is_set():
            try:
                job = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
                
            try:
                await self.process_job(job)
            except asyncio.CancelledError:
                break
            finally:
                queue.task_done()
