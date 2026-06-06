# Camera Daemon

ESP32-CAM video pipeline for Watch. Connects to the camera's MJPEG stream,
detects motion, and dispatches camera events to output subscriptions.

## Structure

- `camera_daemon.py` — main daemon: stream reader, motion detector, stream dispatcher, WebSocket server, snapshot/video archive, HTTP server
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
python camera_daemon.py --url http://192.168.4.1:81/stream --fps 2 --cooldown 5 --threshold 5000 --ws-port 8765
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
  "subscription": "ws-...",
  "timestamp": 1780692497.0,
  "mode": "stills",
  "format": "base64",
  "mediaType": "image/jpeg",
  "payload": "...",
  "metadata": {
    "motion": true,
    "triggered": true,
    "buffer_frames": 6
  }
}
```

For still streams, `mediaType` is `image/jpeg`. For video streams, `mediaType`
is `video/mp4` and each payload is an MP4 encoded from the current rolling
buffer. With `"motionGate": true`, chunks are emitted on motion triggers.

## Architecture (MVP)

1. Frame grabber connects to MJPEG stream
2. Motion detector compares frames at configured FPS
3. Camera loop emits normalized frame events
4. Stream dispatcher routes events to configured subscriptions
5. Built-in archive subscription saves motion JPEGs, rolling MP4 clips, and JSONL events
6. WebSocket server streams Base64 chunks to client subscriptions
7. HTTP server serves archived snapshots, clips, and events

Next: move capture closer to full camera FPS while keeping per-client FPS
throttling in the dispatcher.
