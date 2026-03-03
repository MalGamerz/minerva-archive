import os
import re
import sys
import urllib.parse
import random
import tkinter


def secure_filename(filename: str) -> str:
    filename = os.path.basename(urllib.parse.unquote(filename))
    filename = re.sub(r"\s+", "_", filename)
    filename = re.sub(r"[^a-zA-Z0-9_\-\.\(\)\[\]]", "_", filename)
    return filename.strip("_") or "unnamed_file.bin"


def _retry_sleep(attempt: int, cap: float = 15.0, base: float = 0.85) -> float:
    return min(cap, (base * attempt) + random.random() * 1.25)


def secure_write(path, text: str) -> None:
    """Write text to path, then chmod 600 where supported."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    try:
        os.chmod(path, 0o600)
    except (OSError, NotImplementedError):
        pass  # Windows / some exotic FS — non-fatal


def has_display() -> bool:
    """Return True if a GUI display is available."""
    if sys.platform == "win32":
        return True
    if sys.platform == "darwin":
        return True
    # Linux / BSD: check $DISPLAY / $WAYLAND_DISPLAY, then try Tk
    if os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"):
        try:
            root = tkinter.Tk()
            root.withdraw()
            root.destroy()
            return True
        except Exception:
            return False
    return False


class CustomSpinbox:
    """Minimal +/- spinbox for customtkinter — defined here so both GUI
    entry-points (main.py) can import it from one place."""

    def __init__(self, master, textvariable, width=120, max_val=9999, **kw):
        try:
            import customtkinter as ctk
        except ImportError:
            raise RuntimeError("customtkinter is required for GUI mode")

        self._frame = ctk.CTkFrame(master, fg_color="transparent", width=width, **kw)
        self.textvariable = textvariable
        self.max_val = max_val

        ctk.CTkButton(
            self._frame, text="-", width=28, height=28,
            fg_color="#2B2B2B", hover_color="#3B3B3B", command=self._sub,
        ).pack(side="left", padx=(0, 4))

        ctk.CTkEntry(
            self._frame, textvariable=textvariable,
            width=width - 64, height=28, justify="center",
            fg_color="#181818", border_color="#333",
        ).pack(side="left")

        ctk.CTkButton(
            self._frame, text="+", width=28, height=28,
            fg_color="#2B2B2B", hover_color="#3B3B3B", command=self._add,
        ).pack(side="left", padx=(4, 0))

    def pack(self, **kw):
        self._frame.pack(**kw)

    def grid(self, **kw):
        self._frame.grid(**kw)

    def _add(self):
        try:
            v = int(self.textvariable.get())
            if v < self.max_val:
                self.textvariable.set(str(v + 1))
        except ValueError:
            self.textvariable.set("1")

    def _sub(self):
        try:
            v = int(self.textvariable.get())
            if v > 1:
                self.textvariable.set(str(v - 1))
        except ValueError:
            self.textvariable.set("1")
