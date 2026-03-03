#!/usr/bin/env python3
"""
Minerva DPN Worker — single-file volunteer download client.
Optimized for high-bandwidth, aggressive rate-limit avoidance, and immediate cleanup.
"""

import asyncio
import http.server
import hashlib
import random
import os
import shutil
import sys
import threading
import urllib.parse
import webbrowser
import re
from pathlib import Path

import click
import httpx
from rich.console import Console
from rich.progress import BarColumn, DownloadColumn, Progress, TextColumn, TransferSpeedColumn

# ── Config ──────────────────────────────────────────────────────────────────

VERSION = "1.2.4-max-net"
SERVER_URL = os.environ.get("MINERVA_SERVER", "https://api.minerva-archive.org")
UPLOAD_SERVER_URL = os.environ.get("MINERVA_UPLOAD_SERVER", "https://gate.minerva-archive.org")
TOKEN_FILE = Path.home() / ".minerva-dpn" / "token"
TEMP_DIR = Path.home() / ".minerva-dpn" / "tmp"
MAX_RETRIES = 3
RETRY_DELAY = 5
ARIA2C_SIZE_THRESHOLD = 5 * 1024 * 1024
QUEUE_PREFETCH = 2

console = Console()
ARIA_PROGRESS_REGEX = re.compile(r"\((\d+)%\)")

def auth_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "X-Minerva-Worker-Version": VERSION,
    }

# ── Auth ────────────────────────────────────────────────────────────────────

def save_token(token: str):
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(token)

def load_token() -> str | None:
    if TOKEN_FILE.exists():
        t = TOKEN_FILE.read_text().strip()
        return t if t else None
    return None

def do_login(server_url: str) -> str:
    token = None
    event = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal token
            params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if "token" in params:
                token = params["token"][0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<h1>Logged in! You can close this tab.</h1>")
                event.set()
            else:
                self.send_response(400)
                self.end_headers()

        def log_message(self, *a):
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 19283), Handler)
    srv.timeout = 120

    url = f"{server_url}/auth/discord/login?worker_callback=http://127.0.0.1:19283/"
    console.print(f"[bold]Opening browser for Discord login...")
    console.print(f"[dim]If it doesn't open: {url}")
    webbrowser.open(url)

    while not event.is_set():
        srv.handle_request()
    srv.server_close()

    if not token:
        raise RuntimeError("Login failed")
    save_token(token)
    console.print("[bold green]Login successful!")
    return token

# ── Download ────────────────────────────────────────────────────────────────

HAS_ARIA2C = shutil.which("aria2c") is not None

def _sanitize_component(part: str) -> str:
    bad = '<>:"/\\|?*'
    out = []
    for ch in part:
        if ch in bad or ord(ch) < 32:
            out.append("_")
        else:
            out.append(ch)
    cleaned = "".join(out).strip().rstrip(".")
    return cleaned or "_"

def local_path_for_job(temp_dir: Path, url: str, dest_path: str) -> Path:
    parsed = urllib.parse.urlparse(url)
    host = _sanitize_component(parsed.netloc or "unknown-host")
    decoded_dest = urllib.parse.unquote(dest_path).lstrip("/")
    parts = [_sanitize_component(p) for p in decoded_dest.split("/") if p]
    return temp_dir / host / Path(*parts)

async def download_file(url: str, dest: Path, aria2c_connections: int = 1, known_size: int = 0, progress: Progress = None, tid = None) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    use_aria2c = HAS_ARIA2C and (known_size == 0 or known_size >= ARIA2C_SIZE_THRESHOLD)
    
    # Force 1 connection to prevent 429 rate limit bans
    safe_connections = 1 

    if use_aria2c:
        proc = await asyncio.create_subprocess_exec(
            "aria2c",
            f"--max-connection-per-server={safe_connections}",
            f"--split={safe_connections}",
            "--min-split-size=1M",
            "--dir", str(dest.parent),
            "--out", dest.name,
            "--auto-file-renaming=false",
            "--allow-overwrite=true",
            "--console-log-level=notice",
            "--summary-interval=1",
            "--retry-wait=3",
            "--max-tries=5",
            "--timeout=60",
            "--connect-timeout=15",
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        
        # Real-time progress bar integration
        if progress and tid is not None:
            progress.update(tid, total=100)
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                try:
                    line_str = line.decode('utf-8', errors='replace')
                    match = ARIA_PROGRESS_REGEX.search(line_str)
                    if match:
                        progress.update(tid, completed=int(match.group(1)))
                except Exception:
                    pass

        await proc.wait()
        if proc.returncode != 0:
            raise RuntimeError(f"aria2c exit {proc.returncode}. Server likely refused connection.")
    else:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(connect=15, read=300, write=60, pool=10),
        ) as client:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    async for chunk in resp.aiter_bytes(1024 * 1024):
                        f.write(chunk)
    return dest

# ── Upload ──────────────────────────────────────────────────────────────────

UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024 
UPLOAD_START_RETRIES = 12
UPLOAD_CHUNK_RETRIES = 30
UPLOAD_FINISH_RETRIES = 12
REPORT_RETRIES = 20
RETRIABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}

def _retryable_status(code: int) -> bool:
    return code in RETRIABLE_STATUS_CODES

def _retry_sleep(attempt: int, cap: float = 25.0) -> float:
    return min(cap, (0.85 * attempt) + random.random() * 1.25)

def _raise_if_upgrade_required(resp: httpx.Response):
    if resp.status_code == 426:
        try:
            detail = resp.json().get("detail")
        except Exception:
            detail = resp.text.strip() or "Worker update required"
        raise RuntimeError(detail)

def _response_detail(resp: httpx.Response) -> str:
    try:
        body = resp.json()
        if isinstance(body, dict):
            detail = body.get("detail")
            if detail is not None:
                return str(detail)
    except Exception:
        pass
    return (resp.text or "").strip()

async def upload_file(upload_server_url: str, token: str, file_id: int, path: Path, on_progress=None) -> dict:
    headers = auth_headers(token)
    timeout = httpx.Timeout(connect=30, read=300, write=300, pool=30)
    async with httpx.AsyncClient(timeout=timeout) as client:
        # 1. Start session
        session_id = None
        for attempt in range(1, UPLOAD_START_RETRIES + 1):
            try:
                resp = await client.post(f"{upload_server_url}/api/upload/{file_id}/start", headers=headers)
                _raise_if_upgrade_required(resp)
                if _retryable_status(resp.status_code):
                    if attempt == UPLOAD_START_RETRIES:
                        raise RuntimeError(f"upload start failed ({resp.status_code})")
                    await asyncio.sleep(_retry_sleep(attempt))
                    continue
                resp.raise_for_status()
                session_id = resp.json()["session_id"]
                break
            except httpx.HTTPError as e:
                if attempt == UPLOAD_START_RETRIES:
                    raise RuntimeError(f"upload start failed ({e})") from e
                await asyncio.sleep(_retry_sleep(attempt))
        if not session_id:
            raise RuntimeError("Failed to create upload session")

        # 2. Send chunks
        file_size = path.stat().st_size
        sent = 0
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                data = f.read(UPLOAD_CHUNK_SIZE)
                if not data:
                    break
                hasher.update(data)
                for attempt in range(1, UPLOAD_CHUNK_RETRIES + 1):
                    try:
                        resp = await client.post(
                            f"{upload_server_url}/api/upload/{file_id}/chunk",
                            params={"session_id": session_id},
                            headers={**headers, "Content-Type": "application/octet-stream"},
                            content=data,
                        )
                        _raise_if_upgrade_required(resp)
                        if _retryable_status(resp.status_code):
                            if attempt == UPLOAD_CHUNK_RETRIES:
                                raise RuntimeError(f"upload chunk failed ({resp.status_code})")
                            await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                            continue
                        resp.raise_for_status()
                        break
                    except httpx.HTTPError as e:
                        if attempt == UPLOAD_CHUNK_RETRIES:
                            raise RuntimeError(f"upload chunk failed ({e})") from e
                        await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                sent += len(data)
                if on_progress is not None:
                    on_progress(sent, file_size)

        # 3. Finish
        expected_sha256 = hasher.hexdigest()
        for attempt in range(1, UPLOAD_FINISH_RETRIES + 1):
            try:
                resp = await client.post(
                    f"{upload_server_url}/api/upload/{file_id}/finish",
                    params={"session_id": session_id, "expected_sha256": expected_sha256},
                    headers=headers,
                )
                _raise_if_upgrade_required(resp)
                if _retryable_status(resp.status_code):
                    if attempt == UPLOAD_FINISH_RETRIES:
                        raise RuntimeError(f"upload finish failed ({resp.status_code})")
                    await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                    continue
                resp.raise_for_status()
                result = resp.json()
                break
            except httpx.HTTPError as e:
                if attempt == UPLOAD_FINISH_RETRIES:
                    raise RuntimeError(f"upload finish failed ({e})") from e
                await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
        return result

async def report_job(server_url: str, token: str, file_id: int, status: str, bytes_downloaded: int | None = None, error: str | None = None):
    async with httpx.AsyncClient(timeout=30) as client:
        for attempt in range(1, REPORT_RETRIES + 1):
            try:
                resp = await client.post(
                    f"{server_url}/api/jobs/report",
                    headers=auth_headers(token),
                    json={
                        "file_id": file_id,
                        "status": status,
                        "bytes_downloaded": bytes_downloaded,
                        "error": error,
                    },
                )
                _raise_if_upgrade_required(resp)
                if resp.status_code == 401:
                    raise RuntimeError("Token expired. Run: python worker.py login")
                if resp.status_code == 409 and status == "completed":
                    detail = _response_detail(resp).lower()
                    if "not finalized" in detail or "upload" in detail:
                        if attempt == REPORT_RETRIES:
                            resp.raise_for_status()
                        await asyncio.sleep(min(2.0, 0.25 + attempt * 0.1))
                        continue
                if _retryable_status(resp.status_code):
                    if attempt == REPORT_RETRIES:
                        resp.raise_for_status()
                    await asyncio.sleep(_retry_sleep(attempt, cap=20.0))
                    continue
                resp.raise_for_status()
                return
            except httpx.HTTPError:
                if attempt == REPORT_RETRIES:
                    raise
                await asyncio.sleep(_retry_sleep(attempt, cap=20.0))

# ── Main Loop ───────────────────────────────────────────────────────────────

async def process_job(
    server_url: str,
    upload_server_url: str,
    token: str,
    job: dict,
    temp_dir: Path,
    progress: Progress,
    keep_files: bool,
    aria2c_connections: int,
):
    file_id = job["file_id"]
    url = job["url"]
    dest_path = job["dest_path"]
    label = dest_path[:60] if len(dest_path) <= 60 else "..." + dest_path[-57:]
    known_size = job.get("size", 0) or 0
    tid = progress.add_task(f"[cyan]DL {label}", total=None)
    local_path = local_path_for_job(temp_dir, url, dest_path)

    last_err = None
    file_size = 0
    uploaded = False
    
    # Stagger connections randomly by 1 to 3 seconds to avoid slamming the server 
    await asyncio.sleep(random.uniform(1.0, 3.0))

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Download
            await download_file(url, local_path, aria2c_connections=aria2c_connections, known_size=known_size, progress=progress, tid=tid)
            file_size = local_path.stat().st_size

            # Upload
            progress.update(tid, description=f"[yellow]UL {label}", total=file_size, completed=0)
            await upload_file(
                upload_server_url,
                token,
                file_id,
                local_path,
                on_progress=lambda sent, total: progress.update(tid, completed=sent, total=total),
            )
            uploaded = True
            break
        except Exception as e:
            last_err = e
            local_path.unlink(missing_ok=True) # Delete immediately on failure before retry
            if attempt < MAX_RETRIES:
                err = str(e).splitlines()[0][:72]
                progress.update(tid, description=f"[yellow]RETRY {attempt}/{MAX_RETRIES} {label}")
                console.print(f"[yellow]  {dest_path}: retry {attempt}/{MAX_RETRIES} ({err})")
                await asyncio.sleep(RETRY_DELAY * attempt)

    if not uploaded:
        progress.update(tid, description=f"[red]FAIL {label}")
        try:
            await report_job(server_url, token, file_id, "failed", error=str(last_err)[:500])
        except Exception:
            pass
        console.print(f"[red]  {dest_path}: {last_err}")
        return

    # IMMEDIATE DELETION
    progress.update(tid, description=f"[green]OK {label}", completed=file_size)
    if not keep_files:
        local_path.unlink(missing_ok=True)

    try:
        await report_job(server_url, token, file_id, "completed", bytes_downloaded=file_size)
    except Exception as e:
        console.print(f"[yellow]  {dest_path}: uploaded but report delayed ({str(e)[:120]})")


_STOP = object()

async def worker_loop(
    server_url: str,
    upload_server_url: str,
    token: str,
    temp_dir: Path,
    concurrency: int,
    batch_size: int,
    aria2c_connections: int,
    keep_files: bool,
):
    console.print(f"[bold green]Minerva DPN Worker (Max Net Edition)")
    console.print(f"  Server:      {server_url}")
    console.print(f"  Concurrency: {concurrency} files at once")
    console.print(f"  aria2c conns: 1 (Hardcoded to prevent 429 server bans)")
    console.print(f"  Keep files:  {'yes' if keep_files else 'no (Deleting immediately after upload)'}")
    console.print()

    temp_dir.mkdir(parents=True, exist_ok=True)
    queue: asyncio.Queue = asyncio.Queue(maxsize=concurrency * QUEUE_PREFETCH)
    stop_event = asyncio.Event()
    seen_ids: set[int] = set()

    async def producer():
        no_jobs_warned = False
        async with httpx.AsyncClient(timeout=30) as client:
            while not stop_event.is_set():
                if queue.qsize() >= concurrency:
                    await asyncio.sleep(0.5)
                    continue

                try:
                    resp = await client.get(
                        f"{server_url}/api/jobs",
                        params={"count": min(4, batch_size, max(1, queue.maxsize - queue.qsize()))},
                        headers=auth_headers(token),
                    )
                    if resp.status_code == 426:
                        _raise_if_upgrade_required(resp)
                    if resp.status_code == 401:
                        console.print("[red]Token expired. Run: python worker.py login")
                        stop_event.set()
                        break
                    resp.raise_for_status()
                    data = resp.json()
                    jobs = data["jobs"]
                    if not jobs:
                        if not no_jobs_warned:
                            console.print("[dim]No jobs available, waiting 30s...")
                            no_jobs_warned = True
                        await asyncio.sleep(12 + random.random() * 8)
                        continue

                    no_jobs_warned = False
                    for job in jobs:
                        file_id = job["file_id"]
                        if file_id in seen_ids:
                            continue
                        seen_ids.add(file_id)
                        await queue.put(job)
                except httpx.HTTPError as e:
                    console.print(f"[red]Server error: {e}. Retrying in 10s...")
                    await asyncio.sleep(6 + random.random() * 4)

        for _ in range(concurrency):
            await queue.put(_STOP)

    async def worker(progress: Progress):
        while True:
            job = await queue.get()
            if job is _STOP:
                queue.task_done()
                return
            try:
                await process_job(
                    server_url, upload_server_url, token, job, temp_dir, progress, keep_files, aria2c_connections
                )
            finally:
                seen_ids.discard(job["file_id"])
                queue.task_done()

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(), DownloadColumn(), TransferSpeedColumn(),
        console=console,
    ) as progress:
        workers = [asyncio.create_task(worker(progress)) for _ in range(concurrency)]
        producer_task = asyncio.create_task(producer())
        try:
            await asyncio.gather(producer_task, *workers)
        except KeyboardInterrupt:
            console.print("\n[yellow]Shutting down...")
            stop_event.set()
            producer_task.cancel()
            for t in workers:
                t.cancel()
            return

# ── CLI ─────────────────────────────────────────────────────────────────────

@click.group()
def cli():
    """Minerva DPN Worker — help archive the internet."""
    pass

@cli.command()
@click.option("--server", default=SERVER_URL, help="Manager server URL")
def login(server):
    """Authenticate with Discord."""
    do_login(server)

@cli.command()
@click.option("--server", default=SERVER_URL, help="Manager server URL")
@click.option("--upload-server", default=UPLOAD_SERVER_URL, help="Upload API URL")
@click.option("-c", "--concurrency", default=4, help="Concurrent downloads")
@click.option("-b", "--batch-size", default=10, help="Files per batch")
@click.option("-a", "--aria2c-connections", default=1, help="aria2c connections per file")
@click.option("--temp-dir", default=str(TEMP_DIR), help="Temp download dir")
@click.option("--keep-files", is_flag=True, default=False, help="Keep downloaded files after upload")
def run(server, upload_server, concurrency, batch_size, aria2c_connections, temp_dir, keep_files):
    """Start downloading and uploading files."""
    token = load_token()
    if not token:
        console.print("[red]Not logged in. Run: python worker.py login")
        return
    asyncio.run(
        worker_loop(
            server,
            upload_server,
            token,
            Path(temp_dir),
            concurrency,
            batch_size,
            aria2c_connections,
            keep_files,
        )
    )

if __name__ == "__main__":
    cli()