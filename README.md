# Camera Daemon

ESP32-CAM video pipeline for Watch. Connects to the camera's MJPEG stream,
detects motion, and dispatches camera events to output subscriptions.

## Structure

- `camera_daemon.py` — main daemon: stream reader, motion detector, stream dispatcher, WebSocket server, snapshot/video archive, HTTP server
- `camera_client.py` — WebSocket client fixture for testing stream subscriptions
- `PROTOCOL.md` — WebSocket handshake and message contract
- `requirements.txt` — Python dependencies (opencv-python, requests, websockets)
- `snapshots/` — motion-triggered JPEGs (auto-created)
- `clips/` — motion-triggered rolling MP4 clips (auto-created)
- `camera_events.jsonl` — JSONL event log (auto-created)

## Usage

```bash
# Activate venv
source venv/bin/activate

# Run with defaults
python camera_daemon.py

# Custom config
python camera_daemon.py --url http://192.168.4.1:81/stream --capture-fps 0 --fps 2 --cooldown 5 --threshold 5000 --ws-port 8765

# Test a stream subscription
python camera_client.py --mode stills --motion-gate false --duration 10 --save-dir /tmp/camera-chunks
```

## HTTP Endpoints

- `GET /latest` — latest motion snapshot
- `GET /latest_video` — latest motion video clip
- `GET /events` — all events as JSON

## WebSocket Streaming

Connect to `ws://127.0.0.1:8765/` and send one JSON handshake message:

```json
{
  "mode": "stills",
  "fps": 1,
  "duration": 10,
  "motionGate": true,
  "format": "base64"
}
```

Use `"mode": "video"` to receive Base64 MP4 chunks from the rolling video
buffer instead of Base64 JPEG stills.

The server responds with an `ack`, then sends `chunk` messages:

```json
{
  "type": "chunk",
  "kind": "camera_media_chunk",
  "source": "camera-daemon",
  "subscription": "ws-...",
  "sequence": 1,
  "timestamp": 1780692497.0,
  "capturedAt": "2026-06-05T21:08:17Z",
  "mode": "stills",
  "modality": "image",
  "format": "base64",
  "mediaType": "image/jpeg",
  "payload": "...",
  "dataBase64": "...",
  "sizeBytes": 12345,
  "metadata": {
    "motion": true,
    "triggered": true,
    "buffer_frames": 6
  }
}
```

For still streams, `mediaType` is `image/jpeg`. For video streams, `mediaType`
is `video/mp4` and each payload is an MP4 encoded from the current rolling
buffer. With `"motionGate": true`, chunks are emitted on motion triggers. See
`PROTOCOL.md` for the full Watch-facing contract.

## Architecture (MVP)

1. Frame grabber connects to MJPEG stream
2. Capture loop reads frames continuously, or up to `--capture-fps` when capped
3. Analysis loop samples the latest captured frame at `--fps` for motion detection
4. Stream dispatcher routes normalized frame events to configured subscriptions
5. Built-in archive subscription saves motion JPEGs, rolling MP4 clips, and JSONL events
6. WebSocket server streams Base64 chunks to client subscriptions
7. HTTP server serves archived snapshots, clips, and events

Next: tune continuous video semantics and Watch stream bridge integration.
