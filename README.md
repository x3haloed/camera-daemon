# Camera Daemon

ESP32-CAM video pipeline for Watch. Connects to the camera's MJPEG stream,
detects motion, and outputs events.

## Structure

- `camera_daemon.py` — main daemon: stream reader, motion detector, snapshot output, HTTP server
- `requirements.txt` — Python dependencies (opencv-python, requests)
- `snapshots/` — motion-triggered JPEGs (auto-created)
- `camera_events.jsonl` — JSONL event log (auto-created)

## Usage

```bash
# Activate venv
source venv/bin/activate

# Run with defaults
python camera_daemon.py

# Custom config
python camera_daemon.py --url http://192.168.4.1:81/stream --fps 2 --cooldown 5 --threshold 5000
```

## HTTP Endpoints

- `GET /latest` — latest motion snapshot
- `GET /events` — all events as JSON

## Architecture (MVP)

1. Frame grabber connects to MJPEG stream
2. Motion detector compares frames at configured FPS
3. On motion: save JPEG + write JSONL event
4. HTTP server serves snapshots and events

Phase 2: WebSocket server for multi-client configurable streams.