# Camera Daemon WebSocket Protocol

The daemon exposes camera media as a WebSocket stream intended for Watch
Sounding injection. Clients connect to `ws://127.0.0.1:8765/`, send one JSON
handshake, then receive JSON messages until the client disconnects or the
subscription duration expires.

## Handshake

```json
{
  "mode": "stills",
  "fps": 1,
  "duration": 10,
  "motionGate": true,
  "format": "base64"
}
```

Fields:

- `mode`: `stills` or `video`.
- `fps`: maximum output chunk rate for this subscription.
- `duration`: optional subscription lifetime in seconds.
- `motionGate`: when true, chunks emit only on motion triggers.
- `format`: currently only `base64` is accepted over WebSocket.

## Ack

```json
{
  "type": "ack",
  "subscription": "ws-...",
  "mode": "stills",
  "fps": 1,
  "motionGate": true,
  "format": "base64"
}
```

## Chunk

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
  },
  "hint": "Inject dataBase64 as media in the next Sounding delta."
}
```

Watch-facing fields:

- `kind`: stable discriminator for stream deltas.
- `source`: stable source name.
- `sequence`: per-connection chunk counter.
- `capturedAt`: ISO timestamp for Sounding context.
- `modality`: `image` for stills, `video` for video chunks.
- `mediaType`: `image/jpeg` for stills, `video/mp4` for video chunks.
- `dataBase64`: media payload to inject into a Sounding.
- `sizeBytes`: decoded media size.
- `metadata.motion`: whether motion was detected for the frame event.
- `metadata.triggered`: whether the event passed the motion cooldown gate.
- `metadata.frame_sequence`: camera-daemon capture sequence for the sampled frame.
- `metadata.segment_kind`: `rolling` for motion-gated video clips, `continuous`
  for non-overlapping video segments.
- `metadata.segment_sequence`: per-subscription continuous video segment counter.
- `metadata.segment_start_at` / `metadata.segment_end_at`: ISO segment bounds.
- `metadata.frame_start_sequence` / `metadata.frame_end_sequence`: capture
  sequence bounds for the video segment.

`payload` is retained as a compatibility alias for `dataBase64`.

## Complete

```json
{
  "type": "complete",
  "subscription": "ws-..."
}
```

Sent when a subscription with `duration` reaches its deadline.

## Error

```json
{
  "type": "error",
  "message": "Only base64 format is supported by WebSocket streams"
}
```

## Client Behavior

Clients should reconnect with a fresh handshake if the connection closes.
Subscriptions are connection-scoped and are removed when the socket closes.

For Watch, a stream bridge can subscribe to the daemon, buffer incoming `chunk`
messages, and expose them as stream deltas. The bridge should pass through
`kind`, `source`, `capturedAt`, `modality`, `mediaType`, `dataBase64`,
`sizeBytes`, `sequence`, and `metadata` so a multimodal model can consume the
media without guessing the payload shape.

The daemon captures frames continuously by default. The daemon CLI's `--fps`
controls motion analysis cadence; `--capture-fps` optionally caps camera reads.
Each WebSocket handshake's `fps` controls only that client's maximum output
chunk rate.

## Video Semantics

Video subscriptions have two behaviors:

- With `motionGate: true`, each emitted chunk is a rolling MP4 clip from the
  current frame buffer. This is useful for "what just happened?" motion events.
- With `motionGate: false`, each emitted chunk is a non-overlapping continuous
  MP4 segment. The daemon tracks the last frame sequence emitted to each
  subscription and sends only newly captured frames in the next chunk.
