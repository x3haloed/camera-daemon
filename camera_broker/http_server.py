from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlparse

from .runtime import CameraBroker


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
                if path == "/health":
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
