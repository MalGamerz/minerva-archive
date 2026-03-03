import customtkinter as ctk
import asyncio
import threading
import os
import time
import concurrent.futures

from config import SERVER_URL, UPLOAD_SERVER_URL, TEMP_DIR, BATCH_SIZE
from utils import CustomSpinbox
from auth import MinervaAuth
from engine import WorkerEngine

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")


class MinervaApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Minerva Archive — DPN Worker (Unofficial)")
        self.geometry("1050x650")
        self.minsize(950, 600)

        self.worker_thread: threading.Thread | None = None
        self.worker_engine: WorkerEngine | None = None
        self.token: str | None = MinervaAuth.load_token()

        # Executor is kept at instance level so it can be shut down cleanly.
        self.executor: concurrent.futures.ThreadPoolExecutor | None = None

        self.setup_ui()
        self.update_auth_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ------------------------------------------------------------------
    # Window lifecycle
    # ------------------------------------------------------------------

    def _on_close(self) -> None:
        """Stop the worker gracefully before destroying the window."""
        if self.worker_engine:
            self.worker_engine.stop_event.set()
        self.destroy()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def setup_ui(self) -> None:
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)

        # ── Sidebar ──────────────────────────────────────────────────
        self.sidebar = ctk.CTkFrame(self, width=220, corner_radius=0, fg_color="#181818")
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.sidebar.grid_rowconfigure(10, weight=1)

        ctk.CTkLabel(
            self.sidebar, text="MINERVA", font=ctk.CTkFont(size=24, weight="bold")
        ).grid(row=0, column=0, padx=20, pady=(30, 0))
        ctk.CTkLabel(
            self.sidebar,
            text="DPN Worker (Unofficial)",
            text_color="gray",
            font=ctk.CTkFont(size=11),
        ).grid(row=1, column=0, padx=20, pady=(0, 20))

        self.status_btn = ctk.CTkButton(
            self.sidebar,
            text="🔴 Not Logged In",
            fg_color="#331E1E",
            text_color="#F44336",
            hover=False,
            height=28,
            corner_radius=14,
        )
        self.status_btn.grid(row=2, column=0, padx=20, pady=(0, 30))

        ctk.CTkLabel(
            self.sidebar,
            text="AUTHENTICATION",
            font=ctk.CTkFont(size=10, weight="bold"),
            text_color="gray",
        ).grid(row=3, column=0, padx=20, pady=(10, 5), sticky="w")

        self.login_btn = ctk.CTkButton(
            self.sidebar,
            text="Login with Discord",
            fg_color="#2B2B2B",
            hover_color="#3B3B3B",
            command=self.handle_login,
        )
        self.login_btn.grid(row=4, column=0, padx=20, pady=5)

        self.logout_btn = ctk.CTkButton(
            self.sidebar,
            text="Logout & Delete Token",
            fg_color="#2B2B2B",
            hover_color="#8b1a1a",
            state="disabled",
            command=self.handle_logout,
        )
        self.logout_btn.grid(row=5, column=0, padx=20, pady=5)

        ctk.CTkLabel(
            self.sidebar,
            text="WORKER",
            font=ctk.CTkFont(size=10, weight="bold"),
            text_color="gray",
        ).grid(row=6, column=0, padx=20, pady=(30, 5), sticky="w")

        self.start_btn = ctk.CTkButton(
            self.sidebar,
            text="▶ Start Worker",
            fg_color="#2B2B2B",
            hover_color="#3B3B3B",
            command=self.start_worker,
        )
        self.start_btn.grid(row=7, column=0, padx=20, pady=5)

        self.stop_btn = ctk.CTkButton(
            self.sidebar,
            text="■ Stop Worker",
            fg_color="#991b1b",
            hover_color="#7f1d1d",
            state="disabled",
            command=self.stop_worker,
        )
        self.stop_btn.grid(row=8, column=0, padx=20, pady=5)

        self.run_status = ctk.CTkLabel(
            self.sidebar, text="⚪ Stopped", text_color="gray", font=ctk.CTkFont(size=12)
        )
        self.run_status.grid(row=9, column=0, pady=(10, 0))

        # ── Main content area ─────────────────────────────────────────
        self.main_frame = ctk.CTkFrame(self, fg_color="#0A0A0A", corner_radius=0)
        self.main_frame.grid(row=0, column=1, sticky="nsew")
        self.main_frame.grid_rowconfigure(2, weight=1)
        self.main_frame.grid_columnconfigure(0, weight=1)

        # Settings row
        self.settings_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        self.settings_frame.grid(row=0, column=0, sticky="ew", padx=20, pady=20)
        self.settings_frame.grid_columnconfigure((0, 1, 2), weight=1)

        self.v_api    = ctk.StringVar(value=SERVER_URL)
        self.v_upload = ctk.StringVar(value=UPLOAD_SERVER_URL)
        self.v_conc   = ctk.StringVar(value="4")
        self.v_aria   = ctk.StringVar(value="3")
        self.v_temp   = ctk.StringVar(value=str(TEMP_DIR))
        self.v_keep   = ctk.BooleanVar(value=False)

        self._build_settings_column(
            parent=self.settings_frame, col=0, title="SERVERS",
            rows=[
                ("API Server",     self.v_api,    250),
                ("Upload Server",  self.v_upload, 250),
            ],
        )
        self._build_performance_column(self.settings_frame)
        self._build_storage_column(self.settings_frame)

        # Divider
        ctk.CTkFrame(self.main_frame, height=2, fg_color="#222222").grid(
            row=1, column=0, sticky="ew"
        )

        # Tab view
        self.tabview = ctk.CTkTabview(
            self.main_frame,
            fg_color="transparent",
            segmented_button_selected_color="#333",
            segmented_button_selected_hover_color="#444",
        )
        self.tabview.grid(row=2, column=0, padx=20, pady=10, sticky="nsew")
        self.tabview.add("Active Jobs")
        self.tabview.add("Log")

        self.jobs_scroll = ctk.CTkScrollableFrame(
            self.tabview.tab("Active Jobs"), fg_color="transparent"
        )
        self.jobs_scroll.pack(fill="both", expand=True)

        log_header = ctk.CTkFrame(self.tabview.tab("Log"), fg_color="transparent")
        log_header.pack(fill="x", pady=(0, 5))
        ctk.CTkLabel(
            log_header,
            text="ℹ Log clears on each start",
            text_color="#666666",
            font=("Courier", 11),
        ).pack(side="right", padx=5)

        self.log_box = ctk.CTkTextbox(
            self.tabview.tab("Log"),
            state="disabled",
            font=ctk.CTkFont(family="Courier", size=12),
            fg_color="#111",
        )
        self.log_box.pack(fill="both", expand=True)

        self.job_frames: dict[str, dict] = {}

    def _build_settings_column(self, parent, col: int, title: str, rows: list) -> None:
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.grid(row=0, column=col, sticky="nw", padx=10)
        ctk.CTkLabel(
            frame, text=title, font=ctk.CTkFont(size=11, weight="bold"), text_color="gray"
        ).pack(anchor="w", pady=(0, 10))
        for label, var, width in rows:
            ctk.CTkLabel(frame, text=label, font=ctk.CTkFont(size=12)).pack(anchor="w")
            ctk.CTkEntry(
                frame, textvariable=var, width=width, fg_color="#181818", border_color="#333"
            ).pack(anchor="w", pady=(0, 15))

    def _build_performance_column(self, parent) -> None:
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.grid(row=0, column=1, sticky="nw", padx=10)
        ctk.CTkLabel(
            frame,
            text="PERFORMANCE",
            font=ctk.CTkFont(size=11, weight="bold"),
            text_color="gray",
        ).pack(anchor="w", pady=(0, 10))
        for label, var in [("Concurrency", self.v_conc), ("aria2c Connections", self.v_aria)]:
            ctk.CTkLabel(frame, text=label, font=ctk.CTkFont(size=12)).pack(anchor="w")
            CustomSpinbox(frame, textvariable=var, width=120, max_val=16).pack(
                anchor="w", pady=(0, 15)
            )

    def _build_storage_column(self, parent) -> None:
        frame = ctk.CTkFrame(parent, fg_color="transparent")
        frame.grid(row=0, column=2, sticky="nw", padx=10)
        ctk.CTkLabel(
            frame, text="STORAGE", font=ctk.CTkFont(size=11, weight="bold"), text_color="gray"
        ).pack(anchor="w", pady=(0, 10))
        ctk.CTkLabel(frame, text="Temp Directory", font=ctk.CTkFont(size=12)).pack(anchor="w")
        ctk.CTkEntry(
            frame, textvariable=self.v_temp, width=250, fg_color="#181818", border_color="#333"
        ).pack(anchor="w", pady=(0, 15))
        ctk.CTkCheckBox(
            frame,
            text="Keep files after upload",
            variable=self.v_keep,
            fg_color="#991b1b",
            hover_color="#7f1d1d",
        ).pack(anchor="w", pady=5)

    # ------------------------------------------------------------------
    # Auth state
    # ------------------------------------------------------------------

    def update_auth_ui(self) -> None:
        if self.token:
            self.status_btn.configure(
                text="🟢 Logged in", fg_color="#1E3320", text_color="#4CAF50"
            )
            self.login_btn.configure(state="disabled")
            self.logout_btn.configure(state="normal")
            self.start_btn.configure(
                state="normal", fg_color="#1E3320", hover_color="#2E4A31"
            )
        else:
            self.status_btn.configure(
                text="🔴 Not Logged In", fg_color="#331E1E", text_color="#F44336"
            )
            self.login_btn.configure(state="normal")
            self.logout_btn.configure(state="disabled")
            self.start_btn.configure(state="disabled", fg_color="#2B2B2B")

    # ------------------------------------------------------------------
    # Thread-safe UI helpers
    # ------------------------------------------------------------------

    def log_safe(self, msg: str) -> None:
        def _log():
            self.log_box.configure(state="normal")
            self.log_box.insert("end", f"{time.strftime('%H:%M:%S')} | {msg}\n")
            self.log_box.see("end")
            self.log_box.configure(state="disabled")

        self.after(0, _log)

    def job_new_safe(self, ui_job_id: str, clean_name: str) -> None:
        def _create():
            if ui_job_id in self.job_frames:
                return

            frame = ctk.CTkFrame(
                self.jobs_scroll,
                fg_color="#1A1A1A",
                corner_radius=8,
                border_width=1,
                border_color="#333",
            )
            frame.pack(fill="x", pady=8, padx=5)
            frame.grid_columnconfigure(0, weight=1)

            top = ctk.CTkFrame(frame, fg_color="transparent")
            top.pack(fill="x", padx=15, pady=(12, 5))

            name_lbl = ctk.CTkLabel(
                top,
                text=clean_name,
                text_color="#E0E0E0",
                font=ctk.CTkFont(family="Courier", size=12, weight="bold"),
            )
            name_lbl.pack(side="left")

            speed_lbl = ctk.CTkLabel(
                top, text="0 KB/s", text_color="#4CAF50", font=ctk.CTkFont(size=12)
            )
            speed_lbl.pack(side="right", padx=(15, 0))

            stat_lbl = ctk.CTkLabel(
                top, text="Preparing…", text_color="#AAAAAA", font=ctk.CTkFont(size=12)
            )
            stat_lbl.pack(side="right")

            bar = ctk.CTkProgressBar(
                frame, progress_color="#b22222", fg_color="#333333", height=6
            )
            bar.pack(fill="x", padx=15, pady=(0, 8))
            bar.set(0)

            bottom = ctk.CTkFrame(frame, fg_color="transparent")
            bottom.pack(fill="x", padx=15, pady=(0, 10))

            size_lbl = ctk.CTkLabel(
                bottom, text="Calculating size…", text_color="#888", font=ctk.CTkFont(size=11)
            )
            size_lbl.pack(side="left")

            eta_lbl = ctk.CTkLabel(
                bottom, text="ETA: ∞", text_color="#888", font=ctk.CTkFont(size=11)
            )
            eta_lbl.pack(side="right")

            self.job_frames[ui_job_id] = {
                "frame": frame,
                "bar": bar,
                "stat": stat_lbl,
                "speed": speed_lbl,
                "size": size_lbl,
                "eta": eta_lbl,
            }

        self.after(0, _create)

    def job_update_safe(
        self,
        ui_job_id: str,
        progress_val: float,
        speed_str: str,
        status_text: str,
        size_str: str,
        eta_str: str = "",
    ) -> None:
        def _update():
            widgets = self.job_frames.get(ui_job_id)
            if widgets is None:
                return

            widgets["bar"].set(progress_val)
            widgets["stat"].configure(text=status_text)
            widgets["speed"].configure(text=speed_str)
            widgets["size"].configure(text=size_str)
            if eta_str:
                widgets["eta"].configure(text=eta_str)

            if status_text == "Failed":
                widgets["stat"].configure(text_color="#F44336")
                widgets["bar"].configure(progress_color="#F44336")

            if progress_val >= 1.0 and status_text == "Complete":
                def _destroy():
                    if ui_job_id in self.job_frames:
                        self.job_frames[ui_job_id]["frame"].destroy()
                        self.job_frames.pop(ui_job_id, None)

                self.after(4_000, _destroy)

        self.after(0, _update)

    # ------------------------------------------------------------------
    # Auth handlers
    # ------------------------------------------------------------------

    def handle_login(self) -> None:
        def _login_thread():
            token = MinervaAuth.do_login(self.v_api.get(), self.log_safe)
            if token:
                self.token = token
                self.after(0, self.update_auth_ui)

        threading.Thread(target=_login_thread, daemon=True).start()

    def handle_logout(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            self.stop_worker()
        MinervaAuth.delete_token()
        self.token = None
        self.log_safe("Credentials explicitly revoked and deleted from disk.")
        self.update_auth_ui()

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    def start_worker(self) -> None:
        # Guard against double-start.
        if self.worker_thread and self.worker_thread.is_alive():
            return
        if self.worker_engine is not None:
            return

        # Clear stale UI state.
        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")
        for elements in list(self.job_frames.values()):
            elements["frame"].destroy()
        self.job_frames.clear()

        config = {
            "token":         self.token,
            "api_server":    self.v_api.get(),
            "upload_server": self.v_upload.get(),
            "concurrency":   int(self.v_conc.get()),
            "aria_conns":    int(self.v_aria.get()),
            "batch_size":    BATCH_SIZE,
            "temp_dir":      self.v_temp.get(),
            "keep_files":    self.v_keep.get(),
        }
        callbacks = {
            "log":      self.log_safe,
            "new_job":  self.job_new_safe,
            "progress": self.job_update_safe,
        }

        self.start_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.run_status.configure(text="🟢 Running", text_color="#4CAF50")

        self.worker_engine = WorkerEngine(config, callbacks)

        # Thread pool sized generously for I/O-heavy async executor tasks.
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=os.cpu_count() * 4
        )
        loop = asyncio.new_event_loop()
        loop.set_default_executor(self.executor)

        def _run_async():
            loop.run_until_complete(self.worker_engine.run_loop())
            self.after(0, self._on_worker_stopped)

        self.worker_thread = threading.Thread(target=_run_async, daemon=True)
        self.worker_thread.start()

    def stop_worker(self) -> None:
        if self.worker_engine:
            self.log_safe("Sending stop signal to worker engine…")
            self.worker_engine.stop_event.set()
            self.stop_btn.configure(state="disabled")

    def _on_worker_stopped(self) -> None:
        self.start_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.run_status.configure(text="⚪ Stopped", text_color="gray")
        self.worker_engine = None

        if self.executor:
            self.executor.shutdown(wait=False)
            self.executor = None


if __name__ == "__main__":
    app = MinervaApp()
    app.mainloop()