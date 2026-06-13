from __future__ import annotations

import json
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlparse

from .config import config_to_dict
from .runtime import CameraBroker


STATIC_DIR = Path(__file__).parent / "static"
CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
}


class StatusHTTPServer:
    def __init__(self, broker: CameraBroker, host: str, port: int):
        self.broker = broker
        self.host = host
        self.port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None

    def start(self) -> None:
        broker = self.broker

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                path = urlparse(self.path).path
                if path == "/":
                    self._write_static("index.html")
                elif path == "/app.css":
                    self._write_static("app.css")
                elif path == "/app.js":
                    self._write_static("app.js")
                elif path == "/config":
                    self._write_json({"config": config_to_dict(broker.config)})
                elif path == "/health":
                    self._write_json(broker.status())
                elif path == "/cameras":
                    self._write_json({"cameras": broker.cameras_status()})
                elif path == "/subscriptions":
                    self._write_json({"subscriptions": broker.subscriptions_status()})
                else:
                    self.send_response(404)
                    self.end_headers()
                    self.wfile.write(b"Not found")

            def _write_json(self, payload):
                data = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _write_static(self, filename):
                path = STATIC_DIR / filename
                try:
                    data = path.read_bytes()
                except FileNotFoundError:
                    self.send_response(404)
                    self.end_headers()
                    self.wfile.write(b"Not found")
                    return
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPES.get(path.suffix, "application/octet-stream"))
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, format, *args):
                pass

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self._thread = Thread(target=self._server.serve_forever, name="camera-http", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
