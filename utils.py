import os
import re
import urllib.parse
import random
import customtkinter as ctk


def secure_filename(filename: str) -> str:
    """
    Sanitise a user-supplied or server-supplied filename.

    Defences applied (in order):
      1. URL-decode to collapse percent-encoded traversal tricks (%2F etc.)
      2. Strip any directory component so only the final basename survives.
      3. Collapse whitespace runs to underscores.
      4. Remove every character that isn't alphanumeric or one of: _ - . ( ) [ ]
      5. Strip leading/trailing underscores that result from the above.
      6. Fall back to a safe sentinel when the result is empty.
    """
    filename = os.path.basename(urllib.parse.unquote(filename))
    filename = re.sub(r'\s+', '_', filename)
    filename = re.sub(r'[^a-zA-Z0-9_\-.\(\)\[\]]', '_', filename)
    return filename.strip('_') or "unnamed_file.bin"


def _retry_sleep(attempt: int, cap: float = 15.0, base: float = 0.85) -> float:
    """
    Exponential back-off with full jitter.

    Sleep time grows with each attempt up to *cap* seconds, with a random
    component to spread retries across concurrent workers and avoid thundering
    herds.  The minimum floor is roughly *base* seconds on the first attempt.
    """
    return min(cap, base * attempt + random.random() * 1.25)


class CustomSpinbox(ctk.CTkFrame):
    """A simple integer spinbox built from a CTkEntry flanked by +/- buttons."""

    def __init__(self, master, textvariable, width: int = 120, max_val: int = 9999, **kwargs):
        super().__init__(master, fg_color="transparent", width=width, **kwargs)
        self._var = textvariable
        self._max = max_val

        btn_cfg = dict(width=28, height=28, fg_color="#2B2B2B", hover_color="#3B3B3B")
        self._btn_sub = ctk.CTkButton(self, text="−", command=self._decrement, **btn_cfg)
        self._btn_sub.pack(side="left", padx=(0, 4))

        self._entry = ctk.CTkEntry(
            self,
            textvariable=self._var,
            width=width - 64,
            height=28,
            justify="center",
            fg_color="#181818",
            border_color="#333",
        )
        self._entry.pack(side="left")

        self._btn_add = ctk.CTkButton(self, text="+", command=self._increment, **btn_cfg)
        self._btn_add.pack(side="left", padx=(4, 0))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _current(self) -> int:
        """Return the current integer value, defaulting to 1 on parse error."""
        try:
            return int(self._var.get())
        except ValueError:
            return 1

    def _increment(self) -> None:
        val = self._current()
        self._var.set(str(min(val + 1, self._max)))

    def _decrement(self) -> None:
        val = self._current()
        self._var.set(str(max(val - 1, 1)))