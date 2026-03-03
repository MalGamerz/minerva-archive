import customtkinter as ctk
import asyncio
import threading
import os
import time
import concurrent.futures

from config import SERVER_URL, UPLOAD_SERVER_URL, TEMP_DIR
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
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() * 4)
        loop.set_default_executor(executor)
        
        def _run_async():
            loop.run_until_complete(self.worker_engine.run_loop())
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