import os
import re
import urllib.parse
import random
import customtkinter as ctk

def secure_filename(filename: str) -> str:
    """Prevents Path Traversal and removes illegal characters."""
    filename = os.path.basename(urllib.parse.unquote(filename))
    filename = re.sub(r'[^a-zA-Z0-9_\-\.\(\)\s\[\]]', '_', filename)
    return filename.strip() or "unnamed_file.bin"

def _retry_sleep(attempt: int, cap: float = 15.0) -> float:
    return min(cap, (0.85 * attempt) + random.random() * 1.25)

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