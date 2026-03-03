import os
import threading
import http.server
import urllib.parse
import webbrowser
import secrets
import time

from config import TOKEN_FILE


class MinervaAuth:
    @staticmethod
    def load_token() -> str | None:
        if TOKEN_FILE.exists():
            return TOKEN_FILE.read_text().strip() or None
        return None

    @staticmethod
    def delete_token() -> None:
        if TOKEN_FILE.exists():
            TOKEN_FILE.unlink()

    @staticmethod
    def do_login(server_url: str, log_callback) -> str | None:
        token = None
        event = threading.Event()
        oauth_state = secrets.token_urlsafe(16)

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                # Reject non-loopback connections as a defence-in-depth measure.
                # Note: IPv6 loopback (::1) is also handled if the OS maps it.
                if self.client_address[0] not in ("127.0.0.1", "::1"):
                    self.send_error(403, "Forbidden")
                    return

                nonlocal token
                params = urllib.parse.parse_qs(
                    urllib.parse.urlparse(self.path).query
                )

                if params.get("state", [""])[0] != oauth_state:
                    self.send_response(400)
                    self.send_header("Content-Type", "text/html")
                    self.end_headers()
                    self.wfile.write(b"<h1>Error: Invalid State (CSRF Protection)</h1>")
                    event.set()
                    return

                received = params.get("token", [""])[0].strip()
                if received:
                    token = received
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html")
                    self.end_headers()
                    self.wfile.write(b"<h1>Logged in! You can close this tab.</h1>")
                else:
                    self.send_response(400)
                    self.send_header("Content-Type", "text/html")
                    self.end_headers()
                    self.wfile.write(b"<h1>Error: No token received.</h1>")

                event.set()

            def log_message(self, *args):
                pass  # Suppress default stderr logging

        try:
            srv = http.server.HTTPServer(("127.0.0.1", 19283), Handler)
        except OSError as e:
            log_callback(
                f"Failed to start local login server: port 19283 may already be in use. ({e})"
            )
            return None

        srv.timeout = 1.0  # Allow the poll loop to check the timeout flag

        url = (
            f"{server_url}/auth/discord/login"
            f"?worker_callback=http://127.0.0.1:19283/"
            f"&state={oauth_state}"
        )
        log_callback("Opening browser for secure login…")
        webbrowser.open(url)

        deadline = time.monotonic() + 300  # 5-minute timeout
        while not event.is_set():
            if time.monotonic() > deadline:
                log_callback("Login attempt timed out after 5 minutes.")
                break
            srv.handle_request()

        srv.server_close()

        if token:
            TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
            TOKEN_FILE.write_text(token)
            try:
                os.chmod(TOKEN_FILE, 0o600)
            except OSError:
                pass  # Non-fatal on platforms that don't support chmod (e.g. Windows)
            log_callback("Login successful. Token secured.")

        return token