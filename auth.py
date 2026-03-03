import os
import threading
import http.server
import urllib.parse
import webbrowser
import secrets

from config import TOKEN_FILE

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