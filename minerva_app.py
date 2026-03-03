import customtkinter as ctk
import asyncio
import threading
import os
import shutil
import urllib.parse
import random
import re
import http.server
import webbrowser
import time
import secrets
import hashlib
from pathlib import Path
import httpx

# --- Config & Globals ---
VERSION = "1.2.10-complete"
SERVER_URL = "https://api.minerva-archive.org"
UPLOAD_SERVER_URL = "https://gate.minerva-archive.org"
TOKEN_FILE = Path.home() / ".minerva-dpn" / "token"
TEMP_DIR = Path.home() / ".minerva-dpn" / "tmp"

# Upload Constants
UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024 
UPLOAD_START_RETRIES = 12
UPLOAD_CHUNK_RETRIES = 30
UPLOAD_FINISH_RETRIES = 12
RETRIABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}

# Regex to catch: [Downloaded]/[Total](Percent%) DL:[Speed] ETA:[Time]
ARIA_PROGRESS_REGEX = re.compile(r"\[#\w+\s+([^/]+)/([^\(]+)\((\d+)%\).*?DL:([^\s\]]+)(?:\s+ETA:([^\]]+))?")

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")

# --- Security & Utils ---
def secure_filename(filename: str) -> str:
    """Prevents Path Traversal and removes illegal characters."""
    filename = os.path.basename(urllib.parse.unquote(filename))
    filename = re.sub(r'[^a-zA-Z0-9_\-\.\(\)\s\[\]]', '_', filename)
    return filename.strip() or "unnamed_file.bin"

def _retry_sleep(attempt: int, cap: float = 25.0) -> float:
    return min(cap, (0.85 * attempt) + random.random() * 1.25)

# --- Custom Widgets ---
class CustomSpinbox(ctk.CTkFrame):
    def __init__(self, master, textvariable, width=120, **kwargs):
        super().__init__(master, fg_color="transparent", width=width, **kwargs)
        self.textvariable = textvariable
        
        self.btn_sub = ctk.CTkButton(self, text="-", width=28, height=28, fg_color="#2B2B2B", hover_color="#3B3B3B", command=self.sub)
        self.btn_sub.pack(side="left", padx=(0, 4))
        
        self.entry = ctk.CTkEntry(self, textvariable=self.textvariable, width=width-64, height=28, justify="center", fg_color="#181818", border_color="#333")
        self.entry.pack(side="left")
        
        self.btn_add = ctk.CTkButton(self, text="+", width=28, height=28, fg_color="#2B2B2B", hover_color="#3B3B3B", command=self.add)
        self.btn_add.pack(side="left", padx=(4, 0))

    def add(self):
        try: self.textvariable.set(str(int(self.textvariable.get()) + 1))
        except ValueError: self.textvariable.set("1")

    def sub(self):
        try:
            val = int(self.textvariable.get())
            if val > 1: self.textvariable.set(str(val - 1))
        except ValueError: self.textvariable.set("1")

# --- Backend Engine ---
class MinervaAuth:
    @staticmethod
    def load_token():
        if TOKEN_FILE.exists():
            return TOKEN_FILE.read_text().strip() or None
        return None

    @staticmethod
    def delete_token():
        if TOKEN_FILE.exists():
            TOKEN_FILE.unlink()

    @staticmethod
    def do_login(server_url, log_callback):
        token = None
        event = threading.Event()
        oauth_state = secrets.token_urlsafe(16)

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.client_address[0] != '127.0.0.1':
                    self.send_error(403, "Forbidden")
                    return

                nonlocal token
                params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                
                if params.get("state", [""])[0] != oauth_state:
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"<h1>Error: Invalid State (CSRF Protection)</h1>")
                    event.set()
                    return

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
            def log_message(self, *a): pass

        srv = http.server.HTTPServer(("127.0.0.1", 19283), Handler)
        url = f"{server_url}/auth/discord/login?worker_callback=http://127.0.0.1:19283/&state={oauth_state}"
        log_callback("Opening browser for secure login...")
        webbrowser.open(url)
        
        while not event.is_set():
            srv.handle_request()
        srv.server_close()

        if token:
            TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
            TOKEN_FILE.write_text(token)
            try: os.chmod(TOKEN_FILE, 0o600)
            except Exception: pass 
            log_callback("Login successful. Token secured.")
        return token

class WorkerEngine:
    def __init__(self, config, ui_callbacks):
        self.cfg = config
        self.ui = ui_callbacks 
        self.stop_event = asyncio.Event()
        self.headers = {"Authorization": f"Bearer {config['token']}", "X-Minerva-Worker-Version": VERSION}
        self.aria2c_path = shutil.which("aria2c")

    async def report_job(self, file_id, status, bytes_downloaded=None, error=None):
        async with httpx.AsyncClient(timeout=15.0) as client:
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
                f"--split={self.cfg['aria_conns']}", "--min-split-size=1M",
                "--dir", str(dest.parent), "--out", dest.name,
                "--console-log-level=notice", "--summary-interval=1", "--allow-overwrite=true", 
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
        timeout = httpx.Timeout(connect=30, read=300, write=300, pool=30)
        async with httpx.AsyncClient(timeout=timeout) as client:
            # 1. Start Session
            session_id = None
            for attempt in range(1, UPLOAD_START_RETRIES + 1):
                try:
                    resp = await client.post(f"{self.cfg['upload_server']}/api/upload/{file_id}/start", headers=self.headers)
                    if resp.status_code in RETRIABLE_STATUS_CODES:
                        await asyncio.sleep(_retry_sleep(attempt))
                        continue
                    resp.raise_for_status()
                    session_id = resp.json()["session_id"]
                    break
                except httpx.HTTPError:
                    await asyncio.sleep(_retry_sleep(attempt))
            if not session_id: raise RuntimeError("Failed to create upload session")

            # 2. Send Chunks
            file_size = path.stat().st_size
            sent = 0
            hasher = hashlib.sha256()
            with open(path, "rb") as f:
                while True:
                    data = f.read(UPLOAD_CHUNK_SIZE)
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
                    
                    # Update GUI
                    pct = sent / file_size if file_size > 0 else 1.0
                    self.ui['progress'](
                        ui_job_id, pct, "↑ Uploading", "Uploading", 
                        f"{sent/1024/1024:.1f}MB / {file_size/1024/1024:.1f}MB", "ETA: ..."
                    )

            # 3. Finish
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
        
        self.ui['log'](f"Starting job: {clean_filename}")
        self.ui['new_job'](ui_job_id, clean_filename)

        try:
            await asyncio.sleep(random.uniform(1.0, 3.0))
            await self.download_file(url, local_path, job.get('size', 0), ui_job_id)
            
            file_size = local_path.stat().st_size if local_path.exists() else 0
            
            self.ui['progress'](ui_job_id, 0.0, "Starting Upload...", "Uploading", "Preparing...", "ETA: --")
            await self.upload_file(file_id, local_path, ui_job_id)
            
            self.ui['progress'](ui_job_id, 1.0, "Complete", "Complete", "Upload Finished", "ETA: Done")
            self.ui['log'](f"Successfully processed: {clean_filename}")
            await self.report_job(file_id, "completed", bytes_downloaded=file_size)

        except Exception as e:
            safe_error = str(e).replace(self.cfg['token'], "[REDACTED]") if self.cfg['token'] else str(e)
            self.ui['log'](f"Error on {clean_filename}: {safe_error}")
            self.ui['progress'](ui_job_id, 0.0, "Failed", "Failed", safe_error[:40], "ETA: Error")
            await self.report_job(file_id, "failed", error=safe_error)
        finally:
            if not self.cfg['keep_files']:
                local_path.unlink(missing_ok=True)

    async def run_loop(self):
        queue = asyncio.Queue(maxsize=self.cfg['concurrency'] * 2)
        seen_ids = set()

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

        self.ui['log'](f"Worker engine started with {self.cfg['concurrency']} concurrent slots.")
        prod_task = asyncio.create_task(producer())
        work_tasks = [asyncio.create_task(worker()) for _ in range(self.cfg['concurrency'])]
        
        await self.stop_event.wait()
        prod_task.cancel()
        for t in work_tasks: t.cancel()
        self.ui['log']("Worker engine stopped.")


# --- Frontend GUI ---
class MinervaApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Minerva Archive — DPN Worker (Unofficial)")
        self.geometry("1050x650")
        self.minsize(950, 600)
        
        self.worker_thread = None
        self.worker_engine = None
        self.token = MinervaAuth.load_token()

        self.setup_ui()
        self.update_auth_ui()

    def setup_ui(self):
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)

        self.sidebar = ctk.CTkFrame(self, width=220, corner_radius=0, fg_color="#181818")
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.sidebar.grid_rowconfigure(10, weight=1)

        ctk.CTkLabel(self.sidebar, text="MINERVA", font=ctk.CTkFont(size=24, weight="bold")).grid(row=0, column=0, padx=20, pady=(30, 0))
        ctk.CTkLabel(self.sidebar, text="DPN Worker (Unofficial)", text_color="gray", font=ctk.CTkFont(size=11)).grid(row=1, column=0, padx=20, pady=(0, 20))

        self.status_btn = ctk.CTkButton(self.sidebar, text="🔴 Not Logged In", fg_color="#331E1E", text_color="#F44336", hover=False, height=28, corner_radius=14)
        self.status_btn.grid(row=2, column=0, padx=20, pady=(0, 30))

        ctk.CTkLabel(self.sidebar, text="AUTHENTICATION", font=ctk.CTkFont(size=10, weight="bold"), text_color="gray").grid(row=3, column=0, padx=20, pady=(10, 5), sticky="w")
        self.login_btn = ctk.CTkButton(self.sidebar, text="Login with Discord", fg_color="#2B2B2B", hover_color="#3B3B3B", command=self.handle_login)
        self.login_btn.grid(row=4, column=0, padx=20, pady=5)
        
        self.logout_btn = ctk.CTkButton(self.sidebar, text="Logout & Delete Token", fg_color="#2B2B2B", hover_color="#8b1a1a", state="disabled", command=self.handle_logout)
        self.logout_btn.grid(row=5, column=0, padx=20, pady=5)

        ctk.CTkLabel(self.sidebar, text="WORKER", font=ctk.CTkFont(size=10, weight="bold"), text_color="gray").grid(row=6, column=0, padx=20, pady=(30, 5), sticky="w")
        self.start_btn = ctk.CTkButton(self.sidebar, text="▶ Start Worker", fg_color="#2B2B2B", hover_color="#3B3B3B", command=self.start_worker)
        self.start_btn.grid(row=7, column=0, padx=20, pady=5)
        self.stop_btn = ctk.CTkButton(self.sidebar, text="■ Stop Worker", fg_color="#991b1b", hover_color="#7f1d1d", state="disabled", command=self.stop_worker)
        self.stop_btn.grid(row=8, column=0, padx=20, pady=5)

        self.run_status = ctk.CTkLabel(self.sidebar, text="⚪ Stopped", text_color="gray", font=ctk.CTkFont(size=12))
        self.run_status.grid(row=9, column=0, pady=(10, 0))

        self.main_frame = ctk.CTkFrame(self, fg_color="#0A0A0A", corner_radius=0)
        self.main_frame.grid(row=0, column=1, sticky="nsew", padx=0, pady=0)
        
        self.main_frame.grid_rowconfigure(0, weight=0)
        self.main_frame.grid_rowconfigure(1, weight=0)
        self.main_frame.grid_rowconfigure(2, weight=1)
        self.main_frame.grid_columnconfigure(0, weight=1)

        self.settings_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        self.settings_frame.grid(row=0, column=0, sticky="ew", padx=20, pady=20)
        self.settings_frame.grid_columnconfigure((0, 1, 2), weight=1)

        self.v_api = ctk.StringVar(value=SERVER_URL)
        self.v_upload = ctk.StringVar(value=UPLOAD_SERVER_URL)
        self.v_conc = ctk.StringVar(value="4")
        self.v_aria = ctk.StringVar(value="3")
        self.v_temp = ctk.StringVar(value=str(TEMP_DIR))
        self.v_keep = ctk.BooleanVar(value=False)

        col1 = ctk.CTkFrame(self.settings_frame, fg_color="transparent")
        col1.grid(row=0, column=0, sticky="nw", padx=10)
        ctk.CTkLabel(col1, text="SERVERS", font=ctk.CTkFont(size=11, weight="bold"), text_color="gray").pack(anchor="w", pady=(0, 10))
        ctk.CTkLabel(col1, text="API Server", font=ctk.CTkFont(size=12)).pack(anchor="w")
        ctk.CTkEntry(col1, textvariable=self.v_api, width=250, fg_color="#181818", border_color="#333").pack(anchor="w", pady=(0, 15))
        ctk.CTkLabel(col1, text="Upload Server", font=ctk.CTkFont(size=12)).pack(anchor="w")
        ctk.CTkEntry(col1, textvariable=self.v_upload, width=250, fg_color="#181818", border_color="#333").pack(anchor="w")

        col2 = ctk.CTkFrame(self.settings_frame, fg_color="transparent")
        col2.grid(row=0, column=1, sticky="nw", padx=10)
        ctk.CTkLabel(col2, text="PERFORMANCE", font=ctk.CTkFont(size=11, weight="bold"), text_color="gray").pack(anchor="w", pady=(0, 10))
        ctk.CTkLabel(col2, text="Concurrency", font=ctk.CTkFont(size=12)).pack(anchor="w")
        CustomSpinbox(col2, textvariable=self.v_conc, width=120).pack(anchor="w", pady=(0, 15))
        ctk.CTkLabel(col2, text="aria2c Connections", font=ctk.CTkFont(size=12)).pack(anchor="w")
        CustomSpinbox(col2, textvariable=self.v_aria, width=120).pack(anchor="w")

        col3 = ctk.CTkFrame(self.settings_frame, fg_color="transparent")
        col3.grid(row=0, column=2, sticky="nw", padx=10)
        ctk.CTkLabel(col3, text="STORAGE", font=ctk.CTkFont(size=11, weight="bold"), text_color="gray").pack(anchor="w", pady=(0, 10))
        ctk.CTkLabel(col3, text="Temp Directory", font=ctk.CTkFont(size=12)).pack(anchor="w")
        ctk.CTkEntry(col3, textvariable=self.v_temp, width=250, fg_color="#181818", border_color="#333").pack(anchor="w", pady=(0, 15))
        ctk.CTkCheckBox(col3, text="Keep files after upload", variable=self.v_keep, fg_color="#991b1b", hover_color="#7f1d1d").pack(anchor="w", pady=5)

        divider = ctk.CTkFrame(self.main_frame, height=2, fg_color="#222222")
        divider.grid(row=1, column=0, sticky="ew")

        self.tabview = ctk.CTkTabview(self.main_frame, fg_color="transparent", segmented_button_selected_color="#333", segmented_button_selected_hover_color="#444")
        self.tabview.grid(row=2, column=0, padx=20, pady=10, sticky="nsew")

        self.tabview.add("Active Jobs")
        self.tabview.add("Log")

        self.jobs_scroll = ctk.CTkScrollableFrame(self.tabview.tab("Active Jobs"), fg_color="transparent")
        self.jobs_scroll.pack(fill="both", expand=True)

        self.log_box = ctk.CTkTextbox(self.tabview.tab("Log"), state="disabled", font=ctk.CTkFont(family="Courier", size=12), fg_color="#111")
        self.log_box.pack(fill="both", expand=True)
        
        self.job_frames = {}

    # --- UI Logic & Thread Bridge ---
    def update_auth_ui(self):
        if self.token:
            self.status_btn.configure(text="🟢 Logged in", fg_color="#1E3320", text_color="#4CAF50")
            self.login_btn.configure(state="disabled")
            self.logout_btn.configure(state="normal")
            self.start_btn.configure(state="normal", fg_color="#1E3320", hover_color="#2E4A31")
        else:
            self.status_btn.configure(text="🔴 Not Logged In", fg_color="#331E1E", text_color="#F44336")
            self.login_btn.configure(state="normal")
            self.logout_btn.configure(state="disabled")
            self.start_btn.configure(state="disabled", fg_color="#2B2B2B")

    def log_safe(self, msg):
        def _log():
            self.log_box.configure(state="normal")
            self.log_box.insert("end", f"{time.strftime('%H:%M:%S')} | {msg}\n")
            self.log_box.see("end")
            self.log_box.configure(state="disabled")
        self.after(0, _log)

    def handle_login(self):
        def _login_thread():
            token = MinervaAuth.do_login(self.v_api.get(), self.log_safe)
            if token:
                self.token = token
                self.after(0, self.update_auth_ui)
        threading.Thread(target=_login_thread, daemon=True).start()

    def handle_logout(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.stop_worker()
        MinervaAuth.delete_token()
        self.token = None
        self.log_safe("Credentials explicitly revoked and deleted from disk.")
        self.update_auth_ui()

    def job_new_safe(self, ui_job_id, clean_name):
        def _create():
            if ui_job_id in self.job_frames: return
            
            frame = ctk.CTkFrame(self.jobs_scroll, fg_color="#1A1A1A", corner_radius=8, border_width=1, border_color="#333")
            frame.pack(fill="x", pady=8, padx=5)
            frame.grid_columnconfigure(0, weight=1)
            
            top_container = ctk.CTkFrame(frame, fg_color="transparent")
            top_container.pack(fill="x", padx=15, pady=(12, 5))
            
            lbl = ctk.CTkLabel(top_container, text=clean_name, text_color="#E0E0E0", font=ctk.CTkFont(family="Courier", size=12, weight="bold"))
            lbl.pack(side="left")
            
            speed_lbl = ctk.CTkLabel(top_container, text="0 KB/s", text_color="#4CAF50", font=ctk.CTkFont(size=12))
            speed_lbl.pack(side="right", padx=(15, 0))

            stat_lbl = ctk.CTkLabel(top_container, text="Preparing...", text_color="#AAAAAA", font=ctk.CTkFont(size=12))
            stat_lbl.pack(side="right")
            
            bar = ctk.CTkProgressBar(frame, progress_color="#b22222", fg_color="#333333", height=6)
            bar.pack(fill="x", padx=15, pady=(0, 8))
            bar.set(0)
            
            bottom_container = ctk.CTkFrame(frame, fg_color="transparent")
            bottom_container.pack(fill="x", padx=15, pady=(0, 10))

            size_lbl = ctk.CTkLabel(bottom_container, text="Calculating Size...", text_color="#888", font=ctk.CTkFont(size=11))
            size_lbl.pack(side="left")

            eta_lbl = ctk.CTkLabel(bottom_container, text="ETA: ∞", text_color="#888", font=ctk.CTkFont(size=11))
            eta_lbl.pack(side="right")
            
            self.job_frames[ui_job_id] = {
                "frame": frame, "bar": bar, 
                "stat": stat_lbl, "speed": speed_lbl, "size": size_lbl, "eta": eta_lbl
            }
        self.after(0, _create)

    def job_update_safe(self, ui_job_id, progress_val, speed_str, status_text, size_str, eta_str=""):
        def _update():
            if ui_job_id in self.job_frames:
                self.job_frames[ui_job_id]['bar'].set(progress_val)
                self.job_frames[ui_job_id]['stat'].configure(text=status_text)
                self.job_frames[ui_job_id]['speed'].configure(text=speed_str)
                self.job_frames[ui_job_id]['size'].configure(text=size_str)
                
                if eta_str:
                    self.job_frames[ui_job_id]['eta'].configure(text=eta_str)
                
                if status_text == "Failed":
                    self.job_frames[ui_job_id]['stat'].configure(text_color="#F44336")
                    self.job_frames[ui_job_id]['bar'].configure(progress_color="#F44336")

                if progress_val >= 1.0 and status_text == "Complete":
                    self.after(4000, self.job_frames[ui_job_id]['frame'].destroy)
                    del self.job_frames[ui_job_id]
        self.after(0, _update)

    def start_worker(self):
        if self.worker_thread and self.worker_thread.is_alive(): return
        
        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")
        
        for ui_job_id, elements in list(self.job_frames.items()):
            elements['frame'].destroy()
        self.job_frames.clear()
        
        config = {
            "token": self.token,
            "api_server": self.v_api.get(),
            "upload_server": self.v_upload.get(),
            "concurrency": int(self.v_conc.get()),
            "aria_conns": int(self.v_aria.get()),
            "batch_size": 10,
            "temp_dir": self.v_temp.get(),
            "keep_files": self.v_keep.get()
        }
        
        callbacks = {"log": self.log_safe, "new_job": self.job_new_safe, "progress": self.job_update_safe}

        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.run_status.configure(text="🟢 Running", text_color="#4CAF50")

        self.worker_engine = WorkerEngine(config, callbacks)
        
        def _run_async():
            asyncio.run(self.worker_engine.run_loop())
            self.after(0, self._on_worker_stopped)
            
        self.worker_thread = threading.Thread(target=_run_async, daemon=True)
        self.worker_thread.start()

    def stop_worker(self):
        if self.worker_engine:
            self.log_safe("Sending stop signal to worker engine...")
            self.worker_engine.stop_event.set()
            self.stop_btn.configure(state="disabled")

    def _on_worker_stopped(self):
        self.start_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.run_status.configure(text="⚪ Stopped", text_color="gray")
        self.worker_engine = None

if __name__ == "__main__":
    app = MinervaApp()
    app.mainloop()
