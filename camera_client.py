#!/usr/bin/env python3
"""Small WebSocket client fixture for camera-daemon stream testing."""

import argparse
import asyncio
import base64
import json
from pathlib import Path

import websockets


EXTENSIONS_BY_MEDIA_TYPE = {
    "image/jpeg": ".jpg",
    "video/mp4": ".mp4",
}


async def run_client(args):
    handshake = {
        "mode": args.mode,
        "fps": args.fps,
        "duration": args.duration,
        "motionGate": args.motion_gate,
        "format": "base64",
    }
    if handshake["duration"] is None:
        del handshake["duration"]

    save_dir = Path(args.save_dir) if args.save_dir else None
    if save_dir:
        save_dir.mkdir(parents=True, exist_ok=True)

    async with websockets.connect(args.url) as websocket:
        await websocket.send(json.dumps(handshake))

        while True:
            message = json.loads(await websocket.recv())
            print(json.dumps(summarize_message(message), sort_keys=True))

            if message.get("type") == "chunk" and save_dir:
                save_chunk(save_dir, message)

            if message.get("type") in {"complete", "error"}:
                break


def summarize_message(message):
    if message.get("type") != "chunk":
        return message

    return {
        "type": "chunk",
        "kind": message.get("kind"),
        "subscription": message.get("subscription"),
        "sequence": message.get("sequence"),
        "capturedAt": message.get("capturedAt"),
        "mode": message.get("mode"),
        "modality": message.get("modality"),
        "mediaType": message.get("mediaType"),
        "sizeBytes": message.get("sizeBytes"),
        "metadata": message.get("metadata"),
    }


def save_chunk(save_dir: Path, message):
    media_type = str(message.get("mediaType", "application/octet-stream"))
    extension = EXTENSIONS_BY_MEDIA_TYPE.get(media_type, ".bin")
    sequence = int(message.get("sequence", 0))
    mode = str(message.get("mode", "media"))
    path = save_dir / f"{mode}_{sequence:04d}{extension}"
    path.write_bytes(base64.b64decode(str(message["dataBase64"])))


def main():
    parser = argparse.ArgumentParser(description="Camera daemon WebSocket client fixture")
    parser.add_argument("--url", default="ws://127.0.0.1:8765/", help="WebSocket URL")
    parser.add_argument("--mode", choices=["stills", "video"], default="stills", help="Subscription mode")
    parser.add_argument("--fps", type=float, default=1.0, help="Maximum output chunks per second")
    parser.add_argument("--duration", type=float, help="Optional subscription duration in seconds")
    parser.add_argument("--motion-gate", action=argparse.BooleanOptionalAction, default=True, help="Gate chunks behind motion triggers")
    parser.add_argument("--save-dir", help="Optional directory for decoded media chunks")
    args = parser.parse_args()

    asyncio.run(run_client(args))


if __name__ == "__main__":
    main()
