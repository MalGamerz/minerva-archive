"""
Unified auth module.

GUI mode  → opens browser, spins up a local HTTP server on 127.0.0.1:19283,
            waits for the OAuth redirect.
CLI mode  → same server trick via SSH tunnel, OR falls back to the
            copy-paste flow if the user declines / tunnel isn't available.

Both modes share token load/save/delete.
"""

from __future__ import annotations

import http.server
import os
import secrets
import threading
import time
import urllib.parse
import webbrowser

from config import TOKEN_FILE
from utils import secure_write

# ── Public port for the local OAuth callback ─────────────────────────────────
CALLBACK_PORT = 19283
CALLBACK_HOST = "127.0.0.1"


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

class MinervaAuth:

    # ── Token persistence ─────────────────────────────────────────────────────

    @staticmethod
    def load_token() -> str | None:
        if TOKEN_FILE.exists():
            return TOKEN_FILE.read_text().strip() or None
        return None

    @staticmethod
    def save_token(token: str) -> None:
        secure_write(TOKEN_FILE, token)

    @staticmethod
    def delete_token() -> None:
        if TOKEN_FILE.exists():
            TOKEN_FILE.unlink()

    # ── OAuth callback server ─────────────────────────────────────────────────

    @staticmethod
    def _make_handler(oauth_state: str, token_box: list, event: threading.Event):
        """Return a BaseHTTPRequestHandler class bound to our state."""

        class _Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                # Accept both IPv4 and IPv6 loopback
                if self.client_address[0] not in ("127.0.0.1", "::1"):
                    self.send_error(403, "Forbidden")
                    return

                params = urllib.parse.parse_qs(
                    urllib.parse.urlparse(self.path).query
                )

                if params.get("state", [""])[0] != oauth_state:
                    self._respond(400, b"<h1>Error: Invalid state (CSRF check failed)</h1>")
                    event.set()
                    return

                received = params.get("token", [""])[0].strip()
                if received:
                    token_box.append(received)
                    self._respond(200, b"<h1>Logged in! You may close this tab.</h1>")
                else:
                    self._respond(400, b"<h1>Error: No token in callback.</h1>")
                event.set()

            def _respond(self, code: int, body: bytes):
                self.send_response(code)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass  # silence default stderr noise

        return _Handler

    @staticmethod
    def _start_server(oauth_state: str, token_box: list, event: threading.Event):
        """Try to bind the callback server. Returns (HTTPServer, None) or (None, error_str)."""
        handler = MinervaAuth._make_handler(oauth_state, token_box, event)
        try:
            srv = http.server.HTTPServer((CALLBACK_HOST, CALLBACK_PORT), handler)
            srv.timeout = 1.0
            return srv, None
        except OSError as exc:
            return None, str(exc)

    # ── GUI login (browser redirect) ──────────────────────────────────────────

    @staticmethod
    def do_login(server_url: str, log_callback) -> str | None:
        """
        Full browser-redirect OAuth flow.
        Works on any machine that has a browser reachable at 127.0.0.1:19283
        (direct or via `ssh -R 19283:localhost:19283 …`).
        """
        oauth_state = secrets.token_urlsafe(16)
        token_box: list[str] = []
        event = threading.Event()

        srv, err = MinervaAuth._start_server(oauth_state, token_box, event)
        if srv is None:
            log_callback(
                f"Cannot bind port {CALLBACK_PORT} for OAuth callback: {err}\n"
                f"Tip: nothing else should be using that port, "
                f"or use the headless copy-paste flow instead."
            )
            return None

        url = (
            f"{server_url}/auth/discord/login"
            f"?worker_callback=http://{CALLBACK_HOST}:{CALLBACK_PORT}/"
            f"&state={oauth_state}"
        )
        log_callback("Opening browser for secure login…")
        webbrowser.open(url)

        deadline = time.monotonic() + 300
        while not event.is_set():
            if time.monotonic() > deadline:
                log_callback("Login timed out after 5 minutes.")
                break
            srv.handle_request()
        srv.server_close()

        token = token_box[0] if token_box else None
        if token:
            MinervaAuth.save_token(token)
            log_callback("Login successful. Token secured.")
        return token

    # ── Headless / CLI copy-paste flow ────────────────────────────────────────

    @staticmethod
    def do_login_headless(server_url: str, log_callback) -> str | None:
        """
        No browser needed on this machine.

        Option A (recommended): set up the SSH tunnel first:
            ssh -R 19283:localhost:19283 user@this-server
          Then this function will catch the redirect automatically.

        Option B: visit the URL on any other device, Discord redirects to
          http://127.0.0.1:19283/?token=...&state=...
          which 404s on that device — just copy the full URL and paste it here.
        """
        oauth_state = secrets.token_urlsafe(16)
        token_box: list[str] = []
        event = threading.Event()

        url = (
            f"{server_url}/auth/discord/login"
            f"?worker_callback=http://{CALLBACK_HOST}:{CALLBACK_PORT}/"
            f"&state={oauth_state}"
        )

        # Try to spin up the local server (works if SSH tunnel is active)
        srv, srv_err = MinervaAuth._start_server(oauth_state, token_box, event)

        print("\n" + "=" * 64)
        print(" HEADLESS AUTHENTICATION")
        print("=" * 64)

        if srv is None:
            print(f"  (Could not bind port {CALLBACK_PORT}: {srv_err})")
            print("  Falling back to manual copy-paste flow.\n")
        else:
            print("  SSH tunnel detected (or port is free).")
            print(f"  If you have an active tunnel (`ssh -R {CALLBACK_PORT}:localhost:{CALLBACK_PORT} …`)")
            print("  the login will complete automatically after you authorize.\n")

        print(f"  1. Open this URL in ANY browser (phone, laptop, etc.):\n")
        print(f"     {url}\n")
        print(f"  2. Authorize via Discord.")

        if srv is None:
            print(
                f"  3. Your browser will land on a broken page "
                f"(http://{CALLBACK_HOST}:{CALLBACK_PORT}/…).\n"
                f"     Copy the FULL URL from the address bar and paste it below.\n"
            )
        else:
            print(
                f"  3. If using the SSH tunnel, authorization will be detected automatically.\n"
                f"     Otherwise, paste the redirect URL below when prompted.\n"
            )
        print("=" * 64)

        if srv is not None:
            # Try automatic detection for up to 5 minutes, but also accept paste
            print(
                "  Waiting for automatic redirect "
                "(press Enter to switch to manual paste)…"
            )
            deadline = time.monotonic() + 300

            # Run the server in a background thread so we can also read stdin
            def _serve():
                while not event.is_set() and time.monotonic() < deadline:
                    srv.handle_request()
                srv.server_close()

            t = threading.Thread(target=_serve, daemon=True)
            t.start()

            # Non-blocking stdin check
            import select, sys
            while not event.is_set() and time.monotonic() < deadline:
                # On Windows select() doesn't work on stdin; skip auto-wait there
                if hasattr(select, "select"):
                    r, _, _ = select.select([sys.stdin], [], [], 1.0)
                    if r:
                        break  # user pressed Enter → fall through to paste
                else:
                    time.sleep(1.0)

            if token_box:
                token = token_box[0]
                MinervaAuth.save_token(token)
                log_callback("Login successful (automatic). Token secured.")
                return token

            event.set()  # stop server thread

        # Manual paste fallback
        try:
            redirect_url = input("\n  Paste the redirect URL here: ").strip()
        except (KeyboardInterrupt, EOFError):
            log_callback("Login cancelled.")
            return None

        parsed = urllib.parse.urlparse(redirect_url)
        params = urllib.parse.parse_qs(parsed.query)

        if params.get("state", [""])[0] != oauth_state:
            log_callback("ERROR: State mismatch (CSRF check). Try again.")
            return None

        token = params.get("token", [None])[0]
        if token:
            token = token.strip()
            MinervaAuth.save_token(token)
            log_callback("Login successful. Token secured.")
            return token

        log_callback("ERROR: No token found in the URL.")
        return None
