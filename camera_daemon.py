#!/usr/bin/env python3
"""
Camera Daemon — ESP32-CAM video pipeline for Watch.

Connects to the ESP32-CAM MJPEG stream, decodes frames,
performs motion detection, and outputs events for Watch consumption.

MVP: Motion-gated archive output plus WebSocket still/video subscriptions.
Next: Full-FPS capture with per-client throttling.
"""

import argparse
import asyncio
import base64
from dataclasses import dataclass, field
import json
import logging
import os
from queue import Empty, Full, Queue
import tempfile
import time
from enum import Enum
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from threading import Lock, Thread
from typing import Protocol
from urllib.parse import urlparse
from uuid import uuid4

import cv2
import numpy as np
import websockets
from websockets.exceptions import ConnectionClosed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("camera-daemon")

# ─── Constants ───────────────────────────────────────────────────────────────

SNAPSHOT_DIR = Path("snapshots")
VIDEO_DIR = Path("clips")
EVENTS_FILE = Path("camera_events.jsonl")
DEFAULT_CAMERA_URL = "http://192.168.4.1:81/stream"
DEFAULT_ANALYSIS_FPS = 2  # frames per second to process for motion
DEFAULT_CAPTURE_FPS = 0  # 0 means read as fast as the camera provides frames
DEFAULT_COOLDOWN_S = 5  # seconds between motion triggers
DEFAULT_MOTION_THRESHOLD = 5000  # pixel diff threshold
DEFAULT_HTTP_PORT = 8081  # for serving latest frame
DEFAULT_WS_PORT = 8765  # for streaming subscription clients
DEFAULT_VIDEO_BUFFER_S = 3  # seconds of rolling video buffer


# ─── Stream Events and Subscriptions ─────────────────────────────────────────


class StreamMode(str, Enum):
    STILLS = "stills"
    VIDEO = "video"


class StreamFormat(str, Enum):
    BASE64 = "base64"
    BINARY = "binary"
    FILE = "file"


@dataclass(frozen=True)
class StreamSubscription:
    """Describes what one output listener wants from the camera pipeline."""

    name: str
    mode: StreamMode = StreamMode.STILLS
    fps: float = 1.0
    duration_s: float | None = None
    motion_gate: bool = True
    format: StreamFormat = StreamFormat.BASE64


@dataclass(frozen=True)
class FrameEvent:
    """Normalized frame event emitted by the camera pipeline."""

    frame: np.ndarray
    timestamp: float
    motion: bool
    triggered: bool
    cooldown_s: int
    buffer_frames: int
    frame_sequence: int


@dataclass(frozen=True)
class StreamChunk:
    """One output chunk produced for a stream subscription."""

    subscription: StreamSubscription
    timestamp: float
    media_type: str
    payload: str | bytes
    size_bytes: int
    metadata: dict = field(default_factory=dict)


class StreamSink(Protocol):
    """Receives normalized chunks from the dispatcher."""

    def handle_chunk(self, chunk: StreamChunk, event: FrameEvent, buffer: "FrameBuffer") -> None:
        ...


# ─── MJPEG Stream Reader ────────────────────────────────────────────────────


class MJPEGStreamReader:
    """Reads frames from an MJPEG HTTP stream using OpenCV's VideoCapture."""

    def __init__(self, url: str):
        self.url = url
        self._cap = None

    def connect(self) -> bool:
        """Open the MJPEG stream via OpenCV (handles MJPEG parsing natively)."""
        try:
            log.info(f"Connecting to MJPEG stream: {self.url}")
            self._cap = cv2.VideoCapture(self.url)
            if not self._cap.isOpened():
                log.error("OpenCV failed to open stream")
                return False
            # Warm up — read one frame to verify
            ret, frame = self._cap.read()
            if not ret or frame is None:
                log.error("OpenCV connected but got no frame")
                return False
            log.info(f"Connected to camera stream. Frame: {frame.shape}")
            return True
        except Exception as e:
            log.error(f"Failed to connect to camera: {e}")
            return False

    def next_frame(self):
        """Get the next decoded OpenCV frame (BGR numpy array). Returns None if stream ended."""
        if self._cap is None:
            return None
        try:
            ret, frame = self._cap.read()
            if not ret or frame is None:
                return None
            return frame
        except Exception as e:
            log.error(f"Error reading frame: {e}")
            return None

    def close(self):
        if self._cap:
            self._cap.release()


# ─── Motion Detector ─────────────────────────────────────────────────────────


class MotionDetector:
    """Frame-differencing motion detector."""

    def __init__(self, threshold: int = DEFAULT_MOTION_THRESHOLD, min_area: int = 500):
        self.threshold = threshold
        self.min_area = min_area
        self.prev_gray = None

    def detect(self, frame: np.ndarray) -> tuple[bool, np.ndarray | None]:
        """Detect motion in a frame. Returns (motion_detected, diff_frame)."""
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (21, 21), 0)

        if self.prev_gray is None:
            self.prev_gray = gray
            return False, None

        diff = cv2.absdiff(self.prev_gray, gray)
        thresh = cv2.threshold(diff, 25, 255, cv2.THRESH_BINARY)[1]
        thresh = cv2.dilate(thresh, None, iterations=2)

        contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        total_diff = np.sum(thresh) // 255

        self.prev_gray = gray

        log.debug(f"Motion diff: {total_diff} (threshold: {self.threshold})")

        if total_diff > self.threshold:
            return True, thresh

        return False, None


# ─── Rolling Frame Buffer ─────────────────────────────────────────────────────


class FrameBuffer:
    """Holds a rolling buffer of frames for video clip export."""

    def __init__(self, duration_s: float = DEFAULT_VIDEO_BUFFER_S, fps: int = DEFAULT_ANALYSIS_FPS):
        self.duration_s = duration_s
        self.fps = fps
        self._frames: list[tuple[float, np.ndarray]] = []
        self._lock = Lock()

    def add_frame(self, frame: np.ndarray, timestamp: float | None = None):
        """Add a frame to the rolling buffer."""
        timestamp = timestamp or time.time()
        with self._lock:
            self._frames.append((timestamp, frame))
            cutoff = timestamp - self.duration_s
            self._frames = [(captured_at, buffered_frame) for captured_at, buffered_frame in self._frames if captured_at >= cutoff]

    def dump_to_video(self, output_path: Path) -> bool:
        """Write the current buffer to an MP4 file. Returns True on success."""
        with self._lock:
            frames = list(self._frames)

        return self._write_frames_to_video(frames, output_path)

    def to_video_bytes(self) -> bytes:
        """Encode the current rolling buffer as MP4 bytes."""
        with self._lock:
            frames = list(self._frames)

        if not frames:
            raise RuntimeError("No frames in buffer to encode")

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_file:
            temp_path = Path(temp_file.name)

        try:
            if not self._write_frames_to_video(frames, temp_path):
                raise RuntimeError("Failed to encode video buffer")
            return temp_path.read_bytes()
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass

    def _write_frames_to_video(self, frames: list[tuple[float, np.ndarray]], output_path: Path) -> bool:
        if not frames:
            log.warning("No frames in buffer to write")
            return False

        h, w = frames[0][1].shape[:2]
        fps = estimated_fps(frames, self.fps)
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(str(output_path), fourcc, fps, (w, h))

        for _, frame in frames:
            writer.write(frame)

        writer.release()
        log.info(f"Wrote {len(frames)} frames to {output_path}")
        return True

    @property
    def frame_count(self) -> int:
        with self._lock:
            return len(self._frames)


class LatestFrameStore:
    """Thread-safe holder for the most recent captured frame."""

    def __init__(self):
        self._lock = Lock()
        self._frame: np.ndarray | None = None
        self._timestamp = 0.0
        self._sequence = 0

    def set(self, frame: np.ndarray, timestamp: float) -> int:
        with self._lock:
            self._sequence += 1
            self._frame = frame
            self._timestamp = timestamp
            return self._sequence

    def get(self) -> tuple[np.ndarray, float, int] | None:
        with self._lock:
            if self._frame is None:
                return None
            return self._frame, self._timestamp, self._sequence


def estimated_fps(frames: list[tuple[float, np.ndarray]], fallback: float) -> float:
    if len(frames) < 2:
        return fallback

    duration = frames[-1][0] - frames[0][0]
    if duration <= 0:
        return fallback

    return max(1.0, min(60.0, (len(frames) - 1) / duration))


# ─── Output Manager ──────────────────────────────────────────────────────────


class OutputManager:
    """Manages snapshot storage and event logging."""

    def __init__(self, snapshot_dir: Path = SNAPSHOT_DIR, video_dir: Path = VIDEO_DIR, events_file: Path = EVENTS_FILE):
        self.snapshot_dir = snapshot_dir
        self.video_dir = video_dir
        self.events_file = events_file
        self.snapshot_dir.mkdir(exist_ok=True)
        self.video_dir.mkdir(exist_ok=True)
        self._last_snapshot_path = None
        self._last_video_path = None
        self._lock = Lock()

    def save_snapshot(self, frame: np.ndarray) -> str:
        """Save a frame as JPEG and return the filename."""
        timestamp = int(time.time())
        filename = f"motion_{timestamp}.jpg"
        filepath = self.snapshot_dir / filename
        cv2.imwrite(str(filepath), frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
        with self._lock:
            self._last_snapshot_path = filepath
        return filename

    def log_event(self, event_type: str, filename: str | None = None, details: dict | None = None):
        """Write a JSONL event entry."""
        event = {
            "timestamp": time.time(),
            "type": event_type,
            "snapshot": filename or None,
            **(details or {}),
        }
        with self._lock:
            with open(self.events_file, "a") as f:
                f.write(json.dumps(event) + "\n")
        log.info(f"Event: {event_type} — {filename or 'no snapshot'}")

    @property
    def last_snapshot_path(self) -> str | None:
        with self._lock:
            return str(self._last_snapshot_path) if self._last_snapshot_path else None

    def save_video_clip(self, video_path: Path) -> str:
        """Record a video clip path as the latest."""
        with self._lock:
            self._last_video_path = video_path
        return video_path.name

    @property
    def last_video_path(self) -> str | None:
        with self._lock:
            return str(self._last_video_path) if self._last_video_path else None


# ─── Stream Dispatcher ───────────────────────────────────────────────────────


def encode_frame_as_jpeg(frame: np.ndarray, quality: int = 85) -> bytes:
    """Encode an OpenCV frame as JPEG bytes."""
    ok, encoded = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    if not ok:
        raise RuntimeError("Failed to encode frame as JPEG")
    return encoded.tobytes()


def encode_payload(payload: bytes, stream_format: StreamFormat) -> str | bytes:
    """Encode a media payload according to the subscription format."""
    if stream_format == StreamFormat.BASE64:
        return base64.b64encode(payload).decode("ascii")
    return payload


class FileArchiveSink:
    """Archives motion-triggered still/video chunks for the current HTTP API."""

    def __init__(self, output_manager: OutputManager):
        self.output = output_manager

    def handle_chunk(self, chunk: StreamChunk, event: FrameEvent, buffer: FrameBuffer) -> None:
        if not event.triggered:
            return

        if chunk.subscription.mode == StreamMode.STILLS:
            filename = self.output.save_snapshot(event.frame)
            timestamp = int(event.timestamp)
            video_path = VIDEO_DIR / f"clip_{timestamp}.mp4"
            buffer.dump_to_video(video_path)
            self.output.save_video_clip(video_path)
            self.output.log_event(
                "motion_detected",
                filename=filename,
                details={
                    "cooldown": event.cooldown_s,
                    "video_clip": video_path.name,
                    "buffer_frames": event.buffer_frames,
                    "subscription": chunk.subscription.name,
                },
            )


class StreamDispatcher:
    """Routes camera frame events to configured output subscriptions."""

    def __init__(self, buffer: FrameBuffer):
        self.buffer = buffer
        self._subscriptions: list[StreamSubscription] = []
        self._sinks: list[StreamSink] = []
        self._last_sent: dict[str, float] = {}
        self._lock = Lock()

    def add_subscription(self, subscription: StreamSubscription) -> None:
        with self._lock:
            self._subscriptions.append(subscription)

    def remove_subscription(self, name: str) -> None:
        with self._lock:
            self._subscriptions = [subscription for subscription in self._subscriptions if subscription.name != name]
            self._last_sent.pop(name, None)

    def add_sink(self, sink: StreamSink) -> None:
        with self._lock:
            self._sinks.append(sink)

    def remove_sink(self, sink: StreamSink) -> None:
        with self._lock:
            self._sinks = [existing for existing in self._sinks if existing is not sink]

    def dispatch(self, event: FrameEvent) -> None:
        with self._lock:
            subscriptions = list(self._subscriptions)
            sinks = list(self._sinks)

        for subscription in subscriptions:
            if not self._should_emit(subscription, event):
                continue

            chunk = self._build_chunk(subscription, event)
            self._last_sent[subscription.name] = event.timestamp
            for sink in sinks:
                sink.handle_chunk(chunk, event, self.buffer)

    def _should_emit(self, subscription: StreamSubscription, event: FrameEvent) -> bool:
        if subscription.motion_gate and not event.triggered:
            return False

        if subscription.fps <= 0:
            return False

        if subscription.mode == StreamMode.VIDEO and event.buffer_frames <= 0:
            return False

        last_sent = self._last_sent.get(subscription.name, 0)
        return event.timestamp - last_sent >= 1.0 / subscription.fps

    def _build_chunk(self, subscription: StreamSubscription, event: FrameEvent) -> StreamChunk:
        if subscription.mode == StreamMode.STILLS:
            media_bytes = encode_frame_as_jpeg(event.frame)
            payload = encode_payload(media_bytes, subscription.format)
            return StreamChunk(
                subscription=subscription,
                timestamp=event.timestamp,
                media_type="image/jpeg",
                payload=payload,
                size_bytes=len(media_bytes),
                metadata={
                    "motion": event.motion,
                    "triggered": event.triggered,
                    "buffer_frames": event.buffer_frames,
                    "frame_sequence": event.frame_sequence,
                },
            )

        if subscription.mode == StreamMode.VIDEO:
            media_bytes = self.buffer.to_video_bytes()
            payload = encode_payload(media_bytes, subscription.format)
            return StreamChunk(
                subscription=subscription,
                timestamp=event.timestamp,
                media_type="video/mp4",
                payload=payload,
                size_bytes=len(media_bytes),
                metadata={
                    "motion": event.motion,
                    "triggered": event.triggered,
                    "buffer_frames": event.buffer_frames,
                    "frame_sequence": event.frame_sequence,
                    "duration_s": self.buffer.duration_s,
                },
            )

        raise NotImplementedError(f"Unsupported stream mode: {subscription.mode}")


# ─── WebSocket Streaming ─────────────────────────────────────────────────────


class WebSocketClientSink:
    """Queues chunks for a single WebSocket client."""

    def __init__(self, subscription_name: str, max_queue_size: int = 3):
        self.subscription_name = subscription_name
        self.queue: Queue[dict | None] = Queue(maxsize=max_queue_size)
        self.sequence = 0

    def handle_chunk(self, chunk: StreamChunk, event: FrameEvent, buffer: FrameBuffer) -> None:
        if chunk.subscription.name != self.subscription_name:
            return
        self.sequence += 1
        data_base64 = chunk.payload if isinstance(chunk.payload, str) else base64.b64encode(chunk.payload).decode("ascii")

        message = {
            "type": "chunk",
            "kind": "camera_media_chunk",
            "source": "camera-daemon",
            "subscription": chunk.subscription.name,
            "sequence": self.sequence,
            "timestamp": chunk.timestamp,
            "capturedAt": iso_from_epoch(chunk.timestamp),
            "mode": chunk.subscription.mode.value,
            "modality": modality_for_media_type(chunk.media_type),
            "format": chunk.subscription.format.value,
            "mediaType": chunk.media_type,
            "payload": chunk.payload,
            "dataBase64": data_base64,
            "sizeBytes": chunk.size_bytes,
            "metadata": chunk.metadata,
            "hint": "Inject dataBase64 as media in the next Sounding delta.",
        }

        try:
            self.queue.put_nowait(message)
        except Full:
            try:
                self.queue.get_nowait()
            except Empty:
                pass
            self.queue.put_nowait(message)

    def close(self) -> None:
        try:
            self.queue.put_nowait(None)
        except Full:
            try:
                self.queue.get_nowait()
            except Empty:
                pass
            self.queue.put_nowait(None)


class WebSocketStreamServer:
    """Accepts WebSocket subscription handshakes and streams dispatcher chunks."""

    def __init__(self, dispatcher: StreamDispatcher):
        self.dispatcher = dispatcher

    async def handle_client(self, websocket):
        subscription = None
        sink = None

        try:
            raw = await asyncio.wait_for(websocket.recv(), timeout=10)
            subscription = self._parse_subscription(raw)
            sink = WebSocketClientSink(subscription.name)

            self.dispatcher.add_subscription(subscription)
            self.dispatcher.add_sink(sink)

            await websocket.send(
                json.dumps(
                    {
                        "type": "ack",
                        "subscription": subscription.name,
                        "mode": subscription.mode.value,
                        "fps": subscription.fps,
                        "motionGate": subscription.motion_gate,
                        "format": subscription.format.value,
                    }
                )
            )
            log.info(f"WebSocket subscription connected: {subscription.name}")

            deadline = time.time() + subscription.duration_s if subscription.duration_s else None
            while True:
                if deadline and time.time() >= deadline:
                    await websocket.send(json.dumps({"type": "complete", "subscription": subscription.name}))
                    break

                try:
                    message = await asyncio.wait_for(asyncio.to_thread(sink.queue.get), timeout=1)
                except asyncio.TimeoutError:
                    await websocket.ping()
                    continue

                if message is None:
                    break
                await websocket.send(json.dumps(message))

        except (ConnectionClosed, asyncio.TimeoutError):
            pass
        except ValueError as e:
            await websocket.send(json.dumps({"type": "error", "message": str(e)}))
        finally:
            if subscription:
                self.dispatcher.remove_subscription(subscription.name)
            if sink:
                sink.close()
                self.dispatcher.remove_sink(sink)
            if subscription:
                log.info(f"WebSocket subscription disconnected: {subscription.name}")

    def _parse_subscription(self, raw: str | bytes) -> StreamSubscription:
        try:
            config = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON handshake: {e}") from e

        mode = StreamMode(config.get("mode", StreamMode.STILLS.value))
        stream_format = StreamFormat(config.get("format", StreamFormat.BASE64.value))
        fps = float(config.get("fps", 1))
        duration_s = config.get("duration")
        motion_gate = parse_bool(config.get("motionGate", config.get("motion_gate", True)))

        if mode not in {StreamMode.STILLS, StreamMode.VIDEO}:
            raise ValueError("mode must be 'stills' or 'video'")
        if stream_format != StreamFormat.BASE64:
            raise ValueError("Only base64 format is supported by WebSocket streams")
        if fps <= 0:
            raise ValueError("fps must be greater than 0")

        return StreamSubscription(
            name=f"ws-{uuid4().hex}",
            mode=mode,
            fps=fps,
            duration_s=float(duration_s) if duration_s is not None else None,
            motion_gate=motion_gate,
            format=stream_format,
        )


def parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "on"}
    return bool(value)


def iso_from_epoch(timestamp: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp))


def modality_for_media_type(media_type: str) -> str:
    if media_type.startswith("image/"):
        return "image"
    if media_type.startswith("video/"):
        return "video"
    return "file"


# ─── HTTP Server for Latest Frame ────────────────────────────────────────────


class SnapshotHandler(BaseHTTPRequestHandler):
    """Serves the latest snapshot and video clips."""

    output_manager: OutputManager = None  # type: ignore

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/latest":
            last = self.output_manager.last_snapshot_path
            if last and os.path.exists(last):
                with open(last, "rb") as f:
                    data = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "image/jpeg")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"No snapshot available")
        elif path == "/latest_video":
            last = self.output_manager.last_video_path
            if last and os.path.exists(last):
                with open(last, "rb") as f:
                    data = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "video/mp4")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"No video clip available")
        elif path == "/events":
            try:
                with open(EVENTS_FILE, "r") as f:
                    data = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data.encode())
            except FileNotFoundError:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b"[]")
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")

    def log_message(self, format, *args):
        pass  # Silence HTTP server logs


# ─── Main Daemon Loop ────────────────────────────────────────────────────────


class CameraDaemon:
    """Main daemon orchestrating camera capture, motion detection, and output."""

    def __init__(
        self,
        camera_url: str = DEFAULT_CAMERA_URL,
        analysis_fps: int = DEFAULT_ANALYSIS_FPS,
        capture_fps: float = DEFAULT_CAPTURE_FPS,
        cooldown_s: int = DEFAULT_COOLDOWN_S,
        motion_threshold: int = DEFAULT_MOTION_THRESHOLD,
        http_port: int = DEFAULT_HTTP_PORT,
        ws_port: int = DEFAULT_WS_PORT,
        video_buffer_s: float = DEFAULT_VIDEO_BUFFER_S,
    ):
        self.camera_url = camera_url
        self.analysis_fps = analysis_fps
        self.capture_fps = capture_fps
        self.cooldown_s = cooldown_s
        self.motion_threshold = motion_threshold
        self.http_port = http_port
        self.ws_port = ws_port
        self.running = False

        self.reader = MJPEGStreamReader(camera_url)
        self.detector = MotionDetector(threshold=motion_threshold)
        self.buffer = FrameBuffer(duration_s=video_buffer_s, fps=analysis_fps)
        self.latest_frame = LatestFrameStore()
        self.output = OutputManager()
        self.dispatcher = StreamDispatcher(self.buffer)
        self.dispatcher.add_subscription(
            StreamSubscription(
                name="motion-archive",
                mode=StreamMode.STILLS,
                fps=analysis_fps,
                motion_gate=True,
                format=StreamFormat.FILE,
            )
        )
        self.dispatcher.add_sink(FileArchiveSink(self.output))
        self.websocket_server = WebSocketStreamServer(self.dispatcher)

        # Wire up HTTP handler
        SnapshotHandler.output_manager = self.output

    def start_http(self):
        """Start the HTTP server for serving snapshots."""
        server = HTTPServer(("127.0.0.1", self.http_port), SnapshotHandler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        log.info(f"HTTP server started on http://127.0.0.1:{self.http_port}")

    def start_websocket(self):
        """Start the WebSocket server for streaming subscriptions."""
        thread = Thread(target=lambda: asyncio.run(self._run_websocket_server()), daemon=True)
        thread.start()

    async def _run_websocket_server(self):
        async with websockets.serve(self.websocket_server.handle_client, "127.0.0.1", self.ws_port):
            log.info(f"WebSocket server started on ws://127.0.0.1:{self.ws_port}")
            await asyncio.Future()

    def start_capture(self):
        """Start continuously reading frames from the camera."""
        thread = Thread(target=self._capture_loop, daemon=True)
        thread.start()

    def _capture_loop(self):
        capture_interval = 1.0 / self.capture_fps if self.capture_fps > 0 else 0
        last_capture_time = 0.0

        while self.running:
            frame = self.reader.next_frame()
            if frame is None:
                log.warning("Stream ended, attempting reconnect...")
                if not self.reader.connect():
                    time.sleep(3)
                continue

            now = time.time()
            if capture_interval and now - last_capture_time < capture_interval:
                continue

            last_capture_time = now
            sequence = self.latest_frame.set(frame, now)
            self.buffer.add_frame(frame, now)
            log.debug(f"Captured frame #{sequence}: {frame.shape}")

    def _analysis_frame_generator(self):
        """Sample the latest captured frame at analysis FPS."""
        frame_interval = 1.0 / self.analysis_fps
        last_frame_time = 0
        last_sequence = 0

        while self.running:
            latest = self.latest_frame.get()
            if latest is None:
                time.sleep(0.01)
                continue

            now = time.time()
            if now - last_frame_time < frame_interval:
                time.sleep(min(0.01, frame_interval / 10))
                continue

            frame, captured_at, sequence = latest
            if sequence == last_sequence:
                time.sleep(0.01)
                continue

            last_sequence = sequence
            last_frame_time = now

            log.debug(f"Analyzing frame #{sequence}: {frame.shape}")
            yield frame, captured_at, sequence

    def run(self):
        """Main daemon loop."""
        self.running = True
        self.start_http()
        self.start_websocket()

        if not self.reader.connect():
            log.error("Could not connect to camera. Exiting.")
            return

        self.start_capture()
        log.info("Camera daemon running. Waiting for motion...")
        last_trigger_time = 0

        for frame, captured_at, sequence in self._analysis_frame_generator():
            motion, _ = self.detector.detect(frame)
            now = time.time()
            triggered = motion and (now - last_trigger_time) > self.cooldown_s

            if triggered:
                last_trigger_time = now

            self.dispatcher.dispatch(
                FrameEvent(
                    frame=frame,
                    timestamp=captured_at,
                    motion=motion,
                    triggered=triggered,
                    cooldown_s=self.cooldown_s,
                    buffer_frames=self.buffer.frame_count,
                    frame_sequence=sequence,
                )
            )

    def stop(self):
        self.running = False
        self.reader.close()


# ─── Entry Point ─────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="ESP32-CAM Camera Daemon")
    parser.add_argument("--url", default=DEFAULT_CAMERA_URL, help="MJPEG stream URL")
    parser.add_argument("--fps", type=int, default=DEFAULT_ANALYSIS_FPS, help="Analysis frame rate")
    parser.add_argument("--capture-fps", type=float, default=DEFAULT_CAPTURE_FPS, help="Capture frame rate cap; 0 reads as fast as the camera provides")
    parser.add_argument("--cooldown", type=int, default=DEFAULT_COOLDOWN_S, help="Motion cooldown (seconds)")
    parser.add_argument("--threshold", type=int, default=DEFAULT_MOTION_THRESHOLD, help="Motion sensitivity threshold")
    parser.add_argument("--port", type=int, default=DEFAULT_HTTP_PORT, help="HTTP server port")
    parser.add_argument("--ws-port", type=int, default=DEFAULT_WS_PORT, help="WebSocket streaming port")
    parser.add_argument("--buffer", type=float, default=DEFAULT_VIDEO_BUFFER_S, help="Rolling video buffer duration (seconds)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    daemon = CameraDaemon(
        camera_url=args.url,
        analysis_fps=args.fps,
        capture_fps=args.capture_fps,
        cooldown_s=args.cooldown,
        motion_threshold=args.threshold,
        http_port=args.port,
        ws_port=args.ws_port,
        video_buffer_s=args.buffer,
    )

    try:
        daemon.run()
    except KeyboardInterrupt:
        log.info("Shutting down...")
        daemon.stop()


if __name__ == "__main__":
    main()
