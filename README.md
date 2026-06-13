# Camera Stream Broker

Multi-camera ESP32-CAM stream broker for Watch. The daemon keeps a persistent
JSON camera registry, polls known cameras for liveness, and lazily captures
frames only while WebSocket clients are subscribed.

## Structure

- `camera_daemon.py` - CLI entrypoint
- `camera_broker/` - config, media, runtime, HTTP status, and WebSocket broker code
- `camera_client.py` - WebSocket client fixture for stream subscriptions
- `PROTOCOL.md` - WebSocket handshake and message contract
- `camera_daemon.config.json` - persistent config, auto-created if missing

## Usage

```bash
source venv/bin/activate
python camera_daemon.py
python camera_daemon.py --config camera_daemon.config.json --debug
python camera_client.py --camera-id esp32-cam --mode stills --fps 2 --duration 10
python camera_client.py --camera-id esp32-cam --mode video --clip-seconds 5 --save-dir /tmp/camera-chunks
```

The daemon creates a default config if none exists:

```json
{
  "cameras": [
    {
      "id": "esp32-cam",
      "nickname": "ESP32-CAM",
      "streamUrl": "http://192.168.4.1:81/stream",
      "enabled": true
    }
  ]
}
```

The full generated config also includes server ports, defaults, and guardrails.
Config is file-only in v1; restart the daemon after edits.

## HTTP Status Endpoints

- `GET /health` - daemon status, active counts, and recent in-memory health events
- `GET /cameras` - configured cameras with liveness/runtime state
- `GET /subscriptions` - active subscriptions without media payloads

Media is streamed only over WebSocket. The daemon no longer writes snapshots,
clips, or JSONL events to disk during normal operation.

## WebSocket Streaming

Connect to `ws://127.0.0.1:8765/` and send one subscribe message:

```json
{
  "type": "subscribe",
  "cameraId": "esp32-cam",
  "mode": "stills",
  "fps": 2,
  "resolution": {"width": 640, "height": 480},
  "motionGate": false,
  "motionThreshold": 5000,
  "cooldownSeconds": 5,
  "clipSeconds": 3,
  "durationSeconds": 10
}
```

The server replies with an `ack` containing effective clamped values, then sends
`chunk` messages with Base64 media. `mode: "video"` emits in-memory MP4 chunks;
`mode: "stills"` emits JPEG frames.

## Runtime Model

1. Startup loads or creates JSON config.
2. HTTP status and WebSocket servers start.
3. Enabled cameras are polled for liveness.
4. The first subscription to a camera starts that camera's capture worker.
5. Capture FPS is derived from active subscription demand and clamped by config.
6. Slow clients use bounded queues and drop old chunks instead of blocking capture.
7. The last subscription to a camera stops capture after the idle grace period.
