"""
Minerva DPN Worker — unified entry point.

  python main.py                  → GUI if display available, else CLI help
  python main.py --gui            → force GUI
  python main.py --cli            → force CLI / headless
  python main.py --cli --login    → headless auth
  python main.py --cli --logout
  python main.py --cli --run [options]
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import os
import sys
import threading
import time

from config import SERVER_URL, UPLOAD_SERVER_URL, TEMP_DIR, BATCH_SIZE
from auth import MinervaAuth
from engine import WorkerEngine
from utils import has_display


# ─────────────────────────────────────────────────────────────────────────────
# CLI front-end
# ─────────────────────────────────────────────────────────────────────────────

_last_progress: dict[str, float] = {}


def _cli_log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _cli_new_job(ui_job_id: str, clean_name: str) -> None:
    _cli_log(f">>> QUEUED: {clean_name}")


def _cli_progress(
    ui_job_id: str,
    pct: float,
    speed_str: str,
    status_text: str,
    size_str: str,
    eta_str: str,
) -> None:
    now = time.time()
    if (
        now - _last_progress.get(ui_job_id, 0) > 5.0
        or pct >= 1.0
        or pct == 0.0
    ):
        print(
            f"[{time.strftime('%H:%M:%S')}] [{ui_job_id[:6]}] "
            f"{status_text} | {pct*100:.1f}% | {speed_str} | {size_str} | {eta_str}",
            flush=True,
        )
        _last_progress[ui_job_id] = now


def run_cli(args: argparse.Namespace) -> None:
    if args.logout:
        MinervaAuth.delete_token()
        _cli_log("Credentials deleted.")
        return

    if args.login:
        MinervaAuth.do_login_headless(SERVER_URL, _cli_log)
        return

    if args.run:
        token = MinervaAuth.load_token()
        if not token:
            _cli_log("No token found. Run: python main.py --cli --login")
            sys.exit(1)

        conc = args.concurrency
        aria = args.aria_conns
        if conc * aria > 15:
            _cli_log(
                f"WARNING: {conc} × {aria} = {conc*aria} streams "
                f"exceeds the 15-stream server limit. Adjusting…"
            )
            conc = max(1, 15 // aria)
            _cli_log(f"Concurrency reduced to {conc}.")

        config = {
            "token": token,
            "api_server": SERVER_URL,
            "upload_server": UPLOAD_SERVER_URL,
            "concurrency": conc,
            "aria_conns": aria,
            "batch_size": BATCH_SIZE,
            "temp_dir": str(TEMP_DIR),
            "keep_files": args.keep_files,
        }
        callbacks = {
            "log": _cli_log,
            "new_job": _cli_new_job,
            "progress": _cli_progress,
        }

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        engine = WorkerEngine(config, callbacks)
        
        cpu_count = os.cpu_count() or 1
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count * 4)
        loop.set_default_executor(executor)

        try:
            _cli_log(f"Starting (concurrency={conc}, aria2c_conns={aria})…")
            loop.run_until_complete(engine.run_loop())
        except Exception as e:
            _cli_log(f"FATAL Engine Error: {type(e).__name__}: {e}")
        except KeyboardInterrupt:
            _cli_log("Interrupt received. Stopping…")
            engine.stop_event.set()
            loop.run_until_complete(asyncio.sleep(1))
        finally:
            executor.shutdown(wait=False)
            loop.close()
            _cli_log("Stopped.")
        return

    print(__doc__)


# ─────────────────────────────────────────────────────────────────────────────
# GUI front-end
# ─────────────────────────────────────────────────────────────────────────────

def run_gui() -> None:
    try:
        import customtkinter as ctk
    except ImportError:
        print(
            "customtkinter is not installed.\n"
            "Install it with:  pip install customtkinter\n"
            "Or run headless:  python main.py --cli --run"
        )
        sys.exit(1)

    from utils import CustomSpinbox

    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("dark-blue")

    class MinervaApp(ctk.CTk):

        def __init__(self):
            super().__init__()
            self.title("Minerva Archive — DPN Worker")
            self.geometry("1050x650")
            self.minsize(950, 600)

            self.worker_thread: threading.Thread | None = None
            self.worker_engine: WorkerEngine | None = None
            self.executor: concurrent.futures.ThreadPoolExecutor | None = None
            self.token: str | None = MinervaAuth.load_token()
            self.job_frames: dict[str, dict] = {}
            self._last_gui_update: dict[str, float] = {}

            self._build_ui()
            self._refresh_auth_ui()
            self.protocol("WM_DELETE_WINDOW", self._on_close)

        def _on_close(self) -> None:
            self._stop_worker()
            self.destroy()

        def _build_ui(self) -> None:
            self.grid_rowconfigure(0, weight=1)
            self.grid_columnconfigure(1, weight=1)

            # Sidebar
            sb = ctk.CTkFrame(self, width=220, corner_radius=0, fg_color="#181818")
            sb.grid(row=0, column=0, sticky="nsew")
            sb.grid_rowconfigure(10, weight=1)

            ctk.CTkLabel(sb, text="MINERVA", font=ctk.CTkFont(size=24, weight="bold")).grid(
                row=0, column=0, padx=20, pady=(30, 0)
            )
            ctk.CTkLabel(
                sb, text="DPN Worker", text_color="gray", font=ctk.CTkFont(size=11)
            ).grid(row=1, column=0, padx=20, pady=(0, 20))

            self._status_btn = ctk.CTkButton(
                sb, text="🔴 Not Logged In", fg_color="#331E1E",
                text_color="#F44336", hover=False, height=28, corner_radius=14,
            )
            self._status_btn.grid(row=2, column=0, padx=20, pady=(0, 30))

            ctk.CTkLabel(
                sb, text="AUTHENTICATION", font=ctk.CTkFont(size=10, weight="bold"),
                text_color="gray",
            ).grid(row=3, column=0, padx=20, pady=(10, 5), sticky="w")

            self._login_btn = ctk.CTkButton(
                sb, text="Login with Discord",
                fg_color="#2B2B2B", hover_color="#3B3B3B", command=self._handle_login,
            )
            self._login_btn.grid(row=4, column=0, padx=20, pady=5)

            self._logout_btn = ctk.CTkButton(
                sb, text="Logout & Delete Token",
                fg_color="#2B2B2B", hover_color="#8b1a1a",
                state="disabled", command=self._handle_logout,
            )
            self._logout_btn.grid(row=5, column=0, padx=20, pady=5)

            ctk.CTkLabel(
                sb, text="WORKER", font=ctk.CTkFont(size=10, weight="bold"),
                text_color="gray",
            ).grid(row=6, column=0, padx=20, pady=(30, 5), sticky="w")

            self._start_btn = ctk.CTkButton(
                sb, text="▶ Start Worker",
                fg_color="#2B2B2B", hover_color="#3B3B3B", command=self._start_worker,
            )
            self._start_btn.grid(row=7, column=0, padx=20, pady=5)

            self._stop_btn = ctk.CTkButton(
                sb, text="■ Stop Worker",
                fg_color="#991b1b", hover_color="#7f1d1d",
                state="disabled", command=self._stop_worker,
            )
            self._stop_btn.grid(row=8, column=0, padx=20, pady=5)

            self._run_lbl = ctk.CTkLabel(
                sb, text="⚪ Stopped", text_color="gray", font=ctk.CTkFont(size=12)
            )
            self._run_lbl.grid(row=9, column=0, pady=(10, 0))

            # Main panel
            main = ctk.CTkFrame(self, fg_color="#0A0A0A", corner_radius=0)
            main.grid(row=0, column=1, sticky="nsew")
            main.grid_rowconfigure(2, weight=1)
            main.grid_columnconfigure(0, weight=1)

            # Settings
            sf = ctk.CTkFrame(main, fg_color="transparent")
            sf.grid(row=0, column=0, sticky="ew", padx=20, pady=20)
            sf.grid_columnconfigure((0, 1, 2), weight=1)

            self.v_api    = ctk.StringVar(value=SERVER_URL)
            self.v_upload = ctk.StringVar(value=UPLOAD_SERVER_URL)
            self.v_conc   = ctk.StringVar(value="4")
            self.v_aria   = ctk.StringVar(value="4")
            self.v_temp   = ctk.StringVar(value=str(TEMP_DIR))
            self.v_keep   = ctk.BooleanVar(value=False)

            c0 = ctk.CTkFrame(sf, fg_color="transparent")
            c0.grid(row=0, column=0, sticky="nw", padx=10)
            ctk.CTkLabel(c0, text="SERVERS", font=ctk.CTkFont(size=11, weight="bold"), text_color="gray").pack(anchor="w", pady=(0, 10))
            for lbl, var in [("API Server", self.v_api), ("Upload Server", self.v_upload)]:
                ctk.CTkLabel(c0, text=lbl, font=ctk.CTkFont(size=12)).pack(anchor="w")
                ctk.CTkEntry(c0, textvariable=var, width=250, fg_color="#181818", border_color="#333").pack(anchor="w", pady=(0, 15))

            c1 = ctk.CTkFrame(sf, fg_color="transparent")
            c1.grid(row=0, column=1, sticky="nw", padx=10)
            ctk.CTkLabel(c1, text="PERFORMANCE", font=ctk.CTkFont(size=11, weight="bold"), text_color="gray").pack(anchor="w", pady=(0, 10))
            for lbl, var in [("Concurrency", self.v_conc), ("aria2c Connections", self.v_aria)]:
                ctk.CTkLabel(c1, text=lbl, font=ctk.CTkFont(size=12)).pack(anchor="w")
                CustomSpinbox(c1, textvariable=var, width=120, max_val=16).pack(anchor="w", pady=(0, 15))

            c2 = ctk.CTkFrame(sf, fg_color="transparent")
            c2.grid(row=0, column=2, sticky="nw", padx=10)
            ctk.CTkLabel(c2, text="STORAGE", font=ctk.CTkFont(size=11, weight="bold"), text_color="gray").pack(anchor="w", pady=(0, 10))
            ctk.CTkLabel(c2, text="Temp Directory", font=ctk.CTkFont(size=12)).pack(anchor="w")
            ctk.CTkEntry(c2, textvariable=self.v_temp, width=250, fg_color="#181818", border_color="#333").pack(anchor="w", pady=(0, 15))
            ctk.CTkCheckBox(c2, text="Keep files after upload", variable=self.v_keep, fg_color="#991b1b", hover_color="#7f1d1d").pack(anchor="w")

            ctk.CTkFrame(main, height=2, fg_color="#222222").grid(row=1, column=0, sticky="ew")

            tabs = ctk.CTkTabview(
                main, fg_color="transparent",
                segmented_button_selected_color="#333",
                segmented_button_selected_hover_color="#444",
            )
            tabs.grid(row=2, column=0, padx=20, pady=10, sticky="nsew")
            tabs.add("Active Jobs")
            tabs.add("Log")

            self._jobs_scroll = ctk.CTkScrollableFrame(
                tabs.tab("Active Jobs"), fg_color="transparent"
            )
            self._jobs_scroll.pack(fill="both", expand=True)

            log_hdr = ctk.CTkFrame(tabs.tab("Log"), fg_color="transparent")
            log_hdr.pack(fill="x", pady=(0, 5))
            ctk.CTkLabel(
                log_hdr, text="ℹ Log clears on each start",
                text_color="#666", font=("Courier", 11),
            ).pack(side="right", padx=5)

            self._log_box = ctk.CTkTextbox(
                tabs.tab("Log"), state="disabled",
                font=ctk.CTkFont(family="Courier", size=12), fg_color="#111",
            )
            self._log_box.pack(fill="both", expand=True)

        def _refresh_auth_ui(self) -> None:
            if self.token:
                self._status_btn.configure(text="🟢 Logged in", fg_color="#1E3320", text_color="#4CAF50")
                self._login_btn.configure(state="disabled")
                self._logout_btn.configure(state="normal")
                self._start_btn.configure(state="normal", fg_color="#1E3320", hover_color="#2E4A31")
            else:
                self._status_btn.configure(text="🔴 Not Logged In", fg_color="#331E1E", text_color="#F44336")
                self._login_btn.configure(state="normal")
                self._logout_btn.configure(state="disabled")
                self._start_btn.configure(state="disabled", fg_color="#2B2B2B")

        def log_safe(self, msg: str) -> None:
            def _do():
                self._log_box.configure(state="normal")
                self._log_box.insert("end", f"{time.strftime('%H:%M:%S')} | {msg}\n")
                self._log_box.see("end")
                self._log_box.configure(state="disabled")
            self.after(0, _do)

        def job_new_safe(self, uid: str, name: str) -> None:
            def _do():
                if uid in self.job_frames: return
                import customtkinter as ctk
                frame = ctk.CTkFrame(
                    self._jobs_scroll, fg_color="#1A1A1A",
                    corner_radius=8, border_width=1, border_color="#333",
                )
                frame.pack(fill="x", pady=8, padx=5)
                top = ctk.CTkFrame(frame, fg_color="transparent")
                top.pack(fill="x", padx=15, pady=(12, 5))
                ctk.CTkLabel(top, text=name, text_color="#E0E0E0", font=ctk.CTkFont(family="Courier", size=12, weight="bold")).pack(side="left")
                speed_lbl = ctk.CTkLabel(top, text="0 KB/s", text_color="#4CAF50", font=ctk.CTkFont(size=12))
                speed_lbl.pack(side="right", padx=(15, 0))
                stat_lbl = ctk.CTkLabel(top, text="Preparing…", text_color="#AAAAAA", font=ctk.CTkFont(size=12))
                stat_lbl.pack(side="right")
                bar = ctk.CTkProgressBar(frame, progress_color="#b22222", fg_color="#333333", height=6)
                bar.pack(fill="x", padx=15, pady=(0, 8))
                bar.set(0)
                bot = ctk.CTkFrame(frame, fg_color="transparent")
                bot.pack(fill="x", padx=15, pady=(0, 10))
                size_lbl = ctk.CTkLabel(bot, text="Calculating…", text_color="#888", font=ctk.CTkFont(size=11))
                size_lbl.pack(side="left")
                eta_lbl = ctk.CTkLabel(bot, text="ETA: ∞", text_color="#888", font=ctk.CTkFont(size=11))
                eta_lbl.pack(side="right")
                self.job_frames[uid] = dict(frame=frame, bar=bar, stat=stat_lbl, speed=speed_lbl, size=size_lbl, eta=eta_lbl)
            self.after(0, _do)

        def job_update_safe(self, uid: str, pct: float, speed: str, status: str, size: str, eta: str = "") -> None:
            now = time.time()
            # Hard throttle: max 10 UI updates per second per file to prevent lag
            if status not in ("Complete", "Skipped", "Failed") and 0.0 < pct < 1.0:
                last_time = self._last_gui_update.get(uid, 0)
                if now - last_time < 0.1:
                    return
            self._last_gui_update[uid] = now

            def _do():
                w = self.job_frames.get(uid)
                if not w: return
                w["bar"].set(pct)
                w["stat"].configure(text=status)
                w["speed"].configure(text=speed)
                w["size"].configure(text=size)
                if eta: w["eta"].configure(text=eta)
                if status == "Failed":
                    w["stat"].configure(text_color="#F44336")
                    w["bar"].configure(progress_color="#F44336")
                if pct >= 1.0 and status in ("Complete", "Skipped"):
                    def _rm():
                        if uid in self.job_frames:
                            self.job_frames[uid]["frame"].destroy()
                            self.job_frames.pop(uid, None)
                    self.after(4_000, _rm)
            self.after(0, _do)

        def _handle_login(self) -> None:
            def _t():
                tok = MinervaAuth.do_login(self.v_api.get(), self.log_safe)
                if tok:
                    self.token = tok
                    self.after(0, self._refresh_auth_ui)
            threading.Thread(target=_t, daemon=True).start()

        def _handle_logout(self) -> None:
            if self.worker_thread and self.worker_thread.is_alive():
                self._stop_worker()
            MinervaAuth.delete_token()
            self.token = None
            self.log_safe("Token deleted.")
            self._refresh_auth_ui()

        def _start_worker(self) -> None:
            if (self.worker_thread and self.worker_thread.is_alive()) or self.worker_engine:
                return

            self._log_box.configure(state="normal")
            self._log_box.delete("1.0", "end")
            self._log_box.configure(state="disabled")
            for v in list(self.job_frames.values()):
                v["frame"].destroy()
            self.job_frames.clear()

            config = {
                "token": self.token,
                "api_server": self.v_api.get(),
                "upload_server": self.v_upload.get(),
                "concurrency": int(self.v_conc.get()),
                "aria_conns": int(self.v_aria.get()),
                "batch_size": BATCH_SIZE,
                "temp_dir": self.v_temp.get(),
                "keep_files": self.v_keep.get(),
            }
            callbacks = {
                "log": self.log_safe,
                "new_job": self.job_new_safe,
                "progress": self.job_update_safe,
            }

            self._start_btn.configure(state="disabled")
            self._stop_btn.configure(state="normal")
            self._run_lbl.configure(text="🟢 Running", text_color="#4CAF50")

            def _run():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self._worker_loop = loop  
                
                self.worker_engine = WorkerEngine(config, callbacks)
                
                cpu = os.cpu_count() or 1
                self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=cpu * 4)
                loop.set_default_executor(self.executor)

                try:
                    loop.run_until_complete(self.worker_engine.run_loop())
                except Exception as e:
                    self.log_safe(f"FATAL Engine Error: {type(e).__name__}: {e}")
                finally:
                    self.after(0, self._on_stopped)
                    loop.close()

            self.worker_thread = threading.Thread(target=_run, daemon=True)
            self.worker_thread.start()

        def _stop_worker(self) -> None:
            if getattr(self, "worker_engine", None) and getattr(self, "_worker_loop", None):
                self.log_safe("Stopping…")
                self._worker_loop.call_soon_threadsafe(self.worker_engine.stop_event.set)
                self._stop_btn.configure(state="disabled")

        def _on_stopped(self) -> None:
            self._start_btn.configure(state="normal")
            self._stop_btn.configure(state="disabled")
            self._run_lbl.configure(text="⚪ Stopped", text_color="gray")
            self.worker_engine = None
            if self.executor:
                self.executor.shutdown(wait=False)
                self.executor = None

    app = MinervaApp()
    app.mainloop()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="minerva-worker",
        description="Minerva DPN Worker — auto-selects GUI or CLI based on environment.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--gui", action="store_true", help="Force GUI mode.")
    parser.add_argument("--cli", action="store_true", help="Force CLI/headless mode.")

    cli_group = parser.add_argument_group("CLI options (require --cli)")
    cli_group.add_argument("--login",       action="store_true", help="Headless Discord OAuth.")
    cli_group.add_argument("--logout",      action="store_true", help="Delete stored token.")
    cli_group.add_argument("--run",         action="store_true", help="Start the worker engine.")
    cli_group.add_argument("--concurrency", type=int, default=4,  help="Parallel jobs (default: 4).")
    cli_group.add_argument("--aria-conns",  type=int, default=4,  help="aria2c connections per job (default: 4).")
    cli_group.add_argument("--keep-files",  action="store_true",  help="Keep files after upload.")

    args = parser.parse_args()

    if args.cli or (not args.gui and not has_display()):
        run_cli(args)
    else:
        run_gui()


if __name__ == "__main__":
    main()
