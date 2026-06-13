from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from .config import DEFAULT_CONFIG_PATH, ConfigError, load_or_create_config
from .http_server import StatusHTTPServer
from .runtime import CameraBroker
from .websocket_server import WebSocketStreamServer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("camera-daemon")


class CameraDaemon:
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config = load_or_create_config(config_path)
        self.broker = CameraBroker(self.config)
        self.http = StatusHTTPServer(self.broker, self.config.server.host, self.config.server.http_port)
        self.websocket = WebSocketStreamServer(self.broker, self.config.server.host, self.config.server.ws_port)

    async def run_async(self) -> None:
        self.http.start()
        self.broker.start_health_polling()
        log.info("HTTP status server started on http://%s:%s", self.config.server.host, self.config.server.http_port)
        try:
            await self.websocket.serve_forever()
        finally:
            self.stop()

    def stop(self) -> None:
        self.websocket.stop()
        self.http.stop()
        self.broker.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Camera stream broker")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="JSON config path")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        daemon = CameraDaemon(args.config)
        asyncio.run(daemon.run_async())
    except ConfigError as e:
        log.error("Config error: %s", e)
        raise SystemExit(2) from e
    except KeyboardInterrupt:
        log.info("Shutting down...")
