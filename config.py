import re
import os
from pathlib import Path

VERSION = "2.0.0"
SERVER_URL = "https://api.minerva-archive.org"
UPLOAD_SERVER_URL = "https://gate.minerva-archive.org"
TOKEN_FILE = Path.home() / ".minerva-dpn" / "token"
TEMP_DIR = Path.home() / ".minerva-dpn" / "tmp"

BATCH_SIZE = 10
TEMP_DIR.mkdir(parents=True, exist_ok=True)

UPLOAD_CHUNK_SIZE = 16 * 1024 * 1024          # 16 MiB chunks
UPLOAD_START_RETRIES = 5
UPLOAD_CHUNK_RETRIES = 15
UPLOAD_FINISH_RETRIES = 5
RETRIABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}

# aria2c: saturate the connection as aggressively as possible
ARIA_BASE_ARGS = [
    "--max-connection-per-server=16",   # max per server
    "--split=16",                       # split each file into 16 streams
    "--min-split-size=1M",
    "--piece-length=1M",
    "--max-concurrent-downloads=1",     # one file at a time per invocation
    "--continue=true",
    "--file-allocation=none",           # faster start on all platforms
    "--disk-cache=256M",
    "--optimize-concurrent-downloads=true",
    "--stream-piece-selector=geom",     # geometric selector → faster overall
    "--uri-selector=adaptive",
    "--auto-file-renaming=false",
    "--allow-overwrite=true",
    "--console-log-level=notice",
    "--summary-interval=1",
    "--retry-wait=2",
    "--max-tries=0",                    # retry forever until we cancel
    "--timeout=30",
    "--connect-timeout=10",
    "--lowest-speed-limit=0",           # never abort due to slow speed
]

ARIA_PROGRESS_REGEX = re.compile(
    r"\[#\w+\s+([^/]+)/([^\(]+)\((\d+)%\).*?DL:([^\s\]]+)(?:\s+ETA:([^\]]+))?"
)

PRODUCER_POLL_INTERVAL = 1.0
PRODUCER_BACKOFF_INTERVAL = 10.0

# Windows needs a specific asyncio event loop policy
import sys
if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
