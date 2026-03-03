import asyncio
import os
import shutil
import urllib.parse
import random
import time
import hashlib
import json
import collections
from pathlib import Path
import httpx

from config import (
    VERSION,
    UPLOAD_CHUNK_SIZE,
    UPLOAD_START_RETRIES,
    UPLOAD_CHUNK_RETRIES,
    UPLOAD_FINISH_RETRIES,
    RETRIABLE_STATUS_CODES,
    ARIA_PROGRESS_REGEX,
    PRODUCER_POLL_INTERVAL,
    PRODUCER_BACKOFF_INTERVAL,
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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _redact(self, text: str) -> str:
        """Replace the bearer token with [REDACTED] to avoid leaking it in logs."""
        token = self.cfg.get("token") or ""
        return text.replace(token, "[REDACTED]") if token else text

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    async def report_job(
        self,
        file_id,
        status: str,
        *,
        bytes_downloaded: int | None = None,
        error: str | None = None,
    ) -> None:
        """Best-effort status report; silently drops network failures."""
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
                pass  # Non-fatal — best effort only

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    async def download_file(
        self, url: str, dest: Path, known_size: int, ui_job_id: str
    ) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)

        if self.aria2c_path:
            await self._download_aria2c(url, dest, ui_job_id)
        else:
            self.ui["log"](
                f"[{dest.name}] aria2c not found — using HTTPX fallback."
            )
            await self._download_httpx(url, dest, known_size, ui_job_id)

        return dest

    async def _download_aria2c(self, url: str, dest: Path, ui_job_id: str) -> None:
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
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        async for raw_line in proc.stdout:
            line = raw_line.decode("utf-8", errors="replace")
            match = ARIA_PROGRESS_REGEX.search(line)
            if match:
                self.ui["progress"](
                    ui_job_id,
                    int(match.group(3)) / 100.0,
                    f"{match.group(4).strip()}/s",
                    "Downloading",
                    f"{match.group(1).strip()} / {match.group(2).strip()}",
                    f"ETA: {match.group(5).strip() if match.group(5) else '…'}",
                )

        await proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"aria2c exited with code {proc.returncode}")

    async def _download_httpx(
        self, url: str, dest: Path, known_size: int, ui_job_id: str
    ) -> None:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            async with client.stream("GET", url) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", known_size or 0))
                downloaded = 0
                with open(dest, "wb") as fh:
                    async for chunk in r.aiter_bytes(65_536):
                        fh.write(chunk)
                        downloaded += len(chunk)
                        pct = downloaded / total if total > 0 else 0.0
                        self.ui["progress"](
                            ui_job_id,
                            pct,
                            "Fallback DL",
                            "Downloading",
                            f"{downloaded // 1_048_576} MB / {total // 1_048_576} MB",
                            "ETA: N/A",
                        )

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    async def upload_file(self, file_id, path: Path, ui_job_id: str) -> None:
        timeout = httpx.Timeout(connect=15, read=120, write=120, pool=30)
        async with httpx.AsyncClient(timeout=timeout) as client:
            session_id = await self._upload_start(client, file_id, path)
            await self._upload_chunks(client, file_id, path, session_id, ui_job_id)
            await self._upload_finish(client, file_id, path, session_id)

    async def _upload_start(self, client: httpx.AsyncClient, file_id, path: Path) -> str:
        self.ui["log"](
            f"[{path.name}] Requesting upload session from {self.cfg['upload_server']}…"
        )
        for attempt in range(1, UPLOAD_START_RETRIES + 1):
            try:
                resp = await client.post(
                    f"{self.cfg['upload_server']}/api/upload/{file_id}/start",
                    headers=self.headers,
                )
                
                # Instantly catch a 409 Conflict and abort
                if resp.status_code == 409:
                    raise FileExistsError("409_CONFLICT")

                if resp.status_code in RETRIABLE_STATUS_CODES:
                    self.ui["log"](
                        f"[{path.name}] Upload server busy (HTTP {resp.status_code})."
                        f" Retry {attempt}/{UPLOAD_START_RETRIES}…"
                    )
                    await asyncio.sleep(_retry_sleep(attempt))
                    continue
                resp.raise_for_status()
                return resp.json()["session_id"]
            except FileExistsError:
                raise # Pass it up immediately to the job processor
            except httpx.HTTPError as exc:
                self.ui["log"](f"[{path.name}] Upload connection error: {exc}")
                await asyncio.sleep(_retry_sleep(attempt))

        raise RuntimeError("Failed to obtain upload session after retries.")

    async def _upload_chunks(
        self,
        client: httpx.AsyncClient,
        file_id,
        path: Path,
        session_id: str,
        ui_job_id: str,
    ) -> bytes:
        """Stream file in chunks, retrying each individually. Returns sha256 digest."""
        file_size = path.stat().st_size
        sent = 0
        hasher = hashlib.sha256()
        start_time = time.monotonic()
        loop = asyncio.get_running_loop()

        self.ui["log"](f"[{path.name}] Upload session acquired. Sending data…")

        with open(path, "rb") as fh:
            while True:
                data = await loop.run_in_executor(None, fh.read, UPLOAD_CHUNK_SIZE)
                if not data:
                    break

                hasher.update(data)

                for attempt in range(1, UPLOAD_CHUNK_RETRIES + 1):
                    try:
                        resp = await client.post(
                            f"{self.cfg['upload_server']}/api/upload/{file_id}/chunk",
                            params={"session_id": session_id},
                            headers={
                                **self.headers,
                                "Content-Type": "application/octet-stream",
                            },
                            content=data,
                        )
                        
                        # Catch 409 Conflict mid-upload if another worker finishes first
                        if resp.status_code == 409:
                            raise FileExistsError("409_CONFLICT")

                        if resp.status_code in RETRIABLE_STATUS_CODES:
                            await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                            continue
                        resp.raise_for_status()
                        break  # chunk sent successfully
                    except FileExistsError:
                        raise
                    except httpx.HTTPError:
                        if attempt == UPLOAD_CHUNK_RETRIES:
                            raise RuntimeError(
                                f"Failed to upload chunk after {UPLOAD_CHUNK_RETRIES} attempts."
                            )
                        await asyncio.sleep(_retry_sleep(attempt, cap=20.0))

                sent += len(data)
                elapsed = time.monotonic() - start_time
                speed_bps = sent / elapsed if elapsed > 0 else 0
                speed_mbs = speed_bps / 1_048_576
                remaining = file_size - sent
                eta_sec = remaining / speed_bps if speed_bps > 0 else 0
                eta_m, eta_s = divmod(int(eta_sec), 60)
                pct = sent / file_size if file_size > 0 else 1.0

                self.ui["progress"](
                    ui_job_id,
                    pct,
                    f"↑ {speed_mbs:.1f} MB/s",
                    "Uploading",
                    f"{sent / 1_048_576:.1f} MB / {file_size / 1_048_576:.1f} MB",
                    f"ETA: {eta_m}m {eta_s}s",
                )

        return hasher.hexdigest()

    async def _upload_finish(
        self, client: httpx.AsyncClient, file_id, path: Path, session_id: str
    ) -> None:
        hasher = hashlib.sha256()
        loop = asyncio.get_running_loop()
        with open(path, "rb") as fh:
            while chunk := await loop.run_in_executor(None, fh.read, UPLOAD_CHUNK_SIZE):
                hasher.update(chunk)
        expected_sha256 = hasher.hexdigest()

        self.ui["log"](f"[{path.name}] Upload stream finished. Verifying checksum…")

        for attempt in range(1, UPLOAD_FINISH_RETRIES + 1):
            try:
                resp = await client.post(
                    f"{self.cfg['upload_server']}/api/upload/{file_id}/finish",
                    params={"session_id": session_id, "expected_sha256": expected_sha256},
                    headers=self.headers,
                )
                
                # Catch 409 Conflict right at the finish line
                if resp.status_code == 409:
                    raise FileExistsError("409_CONFLICT")

                if resp.status_code in RETRIABLE_STATUS_CODES:
                    await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                    continue
                resp.raise_for_status()
                return
            except FileExistsError:
                raise
            except httpx.HTTPError:
                if attempt == UPLOAD_FINISH_RETRIES:
                    raise RuntimeError("Failed to finalise upload after retries.")
                await asyncio.sleep(_retry_sleep(attempt, cap=20.0))

    # ------------------------------------------------------------------
    # Job orchestration
    # ------------------------------------------------------------------

    async def process_job(self, job: dict) -> None:
        file_id = job["file_id"]
        url = job["url"]
        raw_dest_path = job["dest_path"]

        parsed = urllib.parse.urlparse(url)
        host = "".join(
            "_" if ch in '<>:"/\\|?*' else ch
            for ch in (parsed.netloc or "unknown")
        ).strip()
        clean_filename = secure_filename(raw_dest_path)

        local_path = Path(self.cfg["temp_dir"]).resolve() / host / clean_filename
        ui_job_id = str(file_id)

        local_path.parent.mkdir(parents=True, exist_ok=True)
        job_cache_file = local_path.with_name(local_path.name + ".job.json")
        with open(job_cache_file, "w") as fh:
            json.dump(job, fh)

        self.ui["log"](f"Starting job: {clean_filename}")
        self.ui["new_job"](ui_job_id, clean_filename)

        try:
            # Small random stagger prevents all concurrent workers hammering the
            # download server at exactly the same instant.
            await asyncio.sleep(random.uniform(0.5, 2.0))
            await self.download_file(url, local_path, job.get("size", 0), ui_job_id)

            file_size = local_path.stat().st_size if local_path.exists() else 0

            self.ui["progress"](ui_job_id, 0.0, "↑ 0.0 MB/s", "Uploading", "Connecting…", "ETA: --")
            await self.upload_file(file_id, local_path, ui_job_id)

            self.ui["progress"](ui_job_id, 1.0, "Complete", "Complete", "Upload Verified", "ETA: Done")
            self.ui["log"](f"Successfully processed: {clean_filename}")
            await self.report_job(file_id, "completed", bytes_downloaded=file_size)

        except asyncio.CancelledError:
            # Let the cancellation propagate so run_loop shuts down cleanly.
            raise
        except FileExistsError:
            # Handle the 409 cleanly without logging it as a scary error
            self.ui["log"](f"[{clean_filename}] Skipped: Server returned 409 Conflict (Already archived by another worker).")
            self.ui["progress"](ui_job_id, 1.0, "Skipped", "Skipped", "Already Archived", "ETA: Done")
            await self.report_job(file_id, "failed", error="409 Conflict")
        except Exception as exc:
            safe_error = self._redact(str(exc))
            self.ui["log"](f"Error on {clean_filename}: {safe_error}")
            self.ui["progress"](ui_job_id, 0.0, "Failed", "Failed", safe_error[:40], "ETA: Error")
            await self.report_job(file_id, "failed", error=safe_error)
        finally:
            job_cache_file.unlink(missing_ok=True)
            local_path.with_name(local_path.name + ".aria2").unlink(missing_ok=True)
            if not self.cfg["keep_files"]:
                local_path.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    async def run_loop(self) -> None:
        queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=self.cfg["concurrency"] * 2)
        # Bounded deque acts as a rolling "seen" cache; avoids unbounded growth.
        seen_ids: collections.deque = collections.deque(maxlen=10_000)

        await self._recover_interrupted_jobs(queue, seen_ids)

        prod_task = asyncio.create_task(self._producer(queue, seen_ids))
        work_tasks = [
            asyncio.create_task(self._worker(queue))
            for _ in range(self.cfg["concurrency"])
        ]

        self.ui["log"](f"Worker engine started. CPU cores available: {os.cpu_count()}")

        await self.stop_event.wait()

        prod_task.cancel()
        for t in work_tasks:
            t.cancel()

        await asyncio.gather(prod_task, *work_tasks, return_exceptions=True)
        self.ui["log"]("Worker engine stopped cleanly.")

    async def _recover_interrupted_jobs(
        self, queue: asyncio.Queue, seen_ids: collections.deque
    ) -> None:
        temp_dir = Path(self.cfg["temp_dir"])
        if not temp_dir.exists():
            return

        recovered = 0
        for job_file in temp_dir.rglob("*.job.json"):
            try:
                with open(job_file) as fh:
                    cached_job = json.load(fh)
                if cached_job["file_id"] not in seen_ids:
                    seen_ids.append(cached_job["file_id"])
                    await queue.put(cached_job)
                    recovered += 1
            except Exception:
                pass  # Corrupt cache file — skip silently

        if recovered:
            self.ui["log"](
                f"Recovered {recovered} interrupted job(s) from local cache. Resuming…"
            )

    async def _producer(
        self, queue: asyncio.Queue, seen_ids: collections.deque
    ) -> None:
        async with httpx.AsyncClient(timeout=10.0) as client:
            while not self.stop_event.is_set():
                # Back-pressure: don't fetch more work than workers can handle.
                if queue.qsize() >= self.cfg["concurrency"]:
                    await asyncio.sleep(PRODUCER_POLL_INTERVAL)
                    continue

                try:
                    resp = await client.get(
                        f"{self.cfg['api_server']}/api/jobs",
                        params={"count": self.cfg["batch_size"]},
                        headers=self.headers,
                    )
                    if resp.status_code == 200:
                        jobs = resp.json().get("jobs", [])
                        for job in jobs:
                            if job["file_id"] not in seen_ids:
                                seen_ids.append(job["file_id"])
                                await queue.put(job)
                        # If server returned nothing, wait before polling again.
                        if not jobs:
                            await asyncio.sleep(PRODUCER_BACKOFF_INTERVAL)
                    else:
                        await asyncio.sleep(PRODUCER_BACKOFF_INTERVAL)
                except httpx.RequestError:
                    await asyncio.sleep(PRODUCER_BACKOFF_INTERVAL)

    async def _worker(self, queue: asyncio.Queue) -> None:
        while not self.stop_event.is_set() or not queue.empty():
            try:
                # Use a short timeout so the loop condition is re-evaluated
                # promptly after stop_event fires.
                job = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            try:
                await self.process_job(job)
            finally:
                queue.task_done()