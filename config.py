import re
from pathlib import Path

VERSION = "1.3.2-resilient"
SERVER_URL = "https://api.minerva-archive.org"
UPLOAD_SERVER_URL = "https://gate.minerva-archive.org"
TOKEN_FILE = Path.home() / ".minerva-dpn" / "token"
TEMP_DIR = Path.home() / ".minerva-dpn" / "tmp"

# Application Config
BATCH_SIZE = 10

# Ensure temp directory exists at boot
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# Upload Constants
UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024
UPLOAD_START_RETRIES = 5
UPLOAD_CHUNK_RETRIES = 10
UPLOAD_FINISH_RETRIES = 5
RETRIABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}

# Producer backoff: how long to wait when queue is full or server returns no jobs (seconds)
PRODUCER_POLL_INTERVAL = 5
PRODUCER_BACKOFF_INTERVAL = 10

# aria2c Regex Parser
ARIA_PROGRESS_REGEX = re.compile(
    r"\[#\w+\s+([^/]+)/([^\(]+)\((\d+)%\).*?DL:([^\s\]]+)(?:\s+ETA:([^\]]+))?"
)