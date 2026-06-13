# Camera Broker WebSocket Protocol

The daemon exposes camera media as JSON WebSocket messages with Base64 payloads.
Clients connect to `ws://127.0.0.1:8765/`, send one `subscribe` message, then
receive media until disconnect or `durationSeconds` expires.

## Subscribe

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

Required fields:

- `type`: must be `subscribe`.
- `cameraId`: camera id from the JSON config.
- `mode`: `stills` or `video`.

Optional fields:

- `fps`: requested output rate.
- `resolution`: requested output width and height.
- `motionGate`: when true, emit only on motion trigger.
- `motionThreshold`: per-subscription motion score threshold.
- `cooldownSeconds`: minimum seconds between motion triggers.
- `clipSeconds`: requested rolling video clip length.
- `durationSeconds`: optional subscription lifetime.

The server clamps requested values against configured limits.

## Ack

```json
{
  "type": "ack",
  "effective": {
    "subscriptionId": "sub-...",
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
}
```

## Chunk

```json
{
  "type": "chunk",
  "kind": "camera_media_chunk",
  "source": "camera-daemon",
  "subscriptionId": "sub-...",
  "cameraId": "esp32-cam",
  "sequence": 1,
  "timestamp": 1780692497.0,
  "capturedAt": "2026-06-05T21:08:17Z",
  "mode": "stills",
  "modality": "image",
  "mediaType": "image/jpeg",
  "dataBase64": "...",
  "sizeBytes": 12345,
  "metadata": {
    "frameSequence": 42,
    "motionScore": 0,
    "triggered": false
  }
}
```

For still streams, `mediaType` is `image/jpeg`. For video streams, `mediaType`
is `video/mp4`. With `motionGate: true`, video chunks are rolling clips from the
current in-memory frame buffer. With `motionGate: false`, video chunks are
continuous non-overlapping segments.

## Complete

```json
{
  "type": "complete",
  "subscriptionId": "sub-..."
}
```

## Error

```json
{
  "type": "error",
  "message": "handshake must be a subscribe message with type='subscribe'"
}
```

Old handshakes without `type: "subscribe"` are intentionally rejected in broker
v1.
