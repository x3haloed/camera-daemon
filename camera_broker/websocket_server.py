from __future__ import annotations

import asyncio
import json
import logging
import time

import websockets
from websockets.exceptions import ConnectionClosed

from .config import ConfigError
from .runtime import CameraBroker, SubscriptionState

log = logging.getLogger("camera-daemon")


class WebSocketStreamServer:
    def __init__(self, broker: CameraBroker, host: str, port: int):
        self.broker = broker
        self.host = host
        self.port = port
        self._stop = asyncio.Event()

    async def serve_forever(self) -> None:
        async with websockets.serve(self.handle_client, self.host, self.port):
            log.info("WebSocket server started on ws://%s:%s", self.host, self.port)
            await self._stop.wait()

    def stop(self) -> None:
        self._stop.set()

    async def handle_client(self, websocket) -> None:
        state: SubscriptionState | None = None
        try:
            raw_message = await asyncio.wait_for(websocket.recv(), timeout=10)
            raw = self._parse_raw(raw_message)
            state = self.broker.subscribe(raw, asyncio.get_running_loop())
            await websocket.send(json.dumps({"type": "ack", "effective": state.effective.as_dict()}))
            log.info("WebSocket subscription connected: %s", state.effective.id)

            deadline = time.time() + state.effective.duration_seconds if state.effective.duration_seconds else None
            while True:
                if deadline and time.time() >= deadline:
                    await websocket.send(json.dumps({"type": "complete", "subscriptionId": state.effective.id}))
                    break
                try:
                    message = await asyncio.wait_for(state.queue.get(), timeout=10)
                except asyncio.TimeoutError:
                    await websocket.ping()
                    continue
                await websocket.send(json.dumps(message))
        except (ConnectionClosed, asyncio.TimeoutError):
            pass
        except (ConfigError, ValueError) as e:
            try:
                await websocket.send(json.dumps({"type": "error", "message": str(e)}))
            except ConnectionClosed:
                pass
        finally:
            if state:
                self.broker.unsubscribe(state)
                log.info("WebSocket subscription disconnected: %s", state.effective.id)

    def _parse_raw(self, raw_message) -> dict:
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")
        try:
            parsed = json.loads(raw_message)
        except json.JSONDecodeError as e:
            raise ValueError(f"invalid JSON handshake: {e}") from e
        if not isinstance(parsed, dict):
            raise ValueError("handshake must be a JSON object")
        return parsed
