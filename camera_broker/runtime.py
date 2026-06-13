from __future__ import annotations

import asyncio
import base64
from collections import deque
from dataclasses import dataclass
import logging
from threading import Event, Lock, Thread, Timer
import time
from typing import Any
from uuid import uuid4

from .config import ConfigError
from .media import (
    FrameBuffer,
    MotionDetector,
    clamp_resolution,
    encode_frame_as_jpeg,
    encode_frames_as_mp4,
    resize_frame,
)
from .models import (
    AppConfig,
    CameraConfig,
    EffectiveSubscription,
    HealthEvent,
    MediaChunk,
    Resolution,
    StreamMode,
)
from .reader import MJPEGStreamReader, probe_stream_liveness

log = logging.getLogger("camera-daemon")


@dataclass
class SubscriptionState:
    effective: EffectiveSubscription
    queue: asyncio.Queue
    loop: asyncio.AbstractEventLoop
    sent_sequence: int = 0
    last_sent_at: float = 0.0
    last_trigger_at: float = 0.0
    last_video_sequence: int = 0
    video_segment_sequence: int = 0


class CameraRuntime:
    def __init__(self, camera: CameraConfig, config: AppConfig, health_callback):
        self.camera = camera
        self.config = config
        self.reader = MJPEGStreamReader(camera.stream_url)
        self.detector = MotionDetector()
        self.buffer = FrameBuffer(config.limits.max_clip_seconds, config.defaults.fps)
        self._health_callback = health_callback
        self._subscriptions: dict[str, SubscriptionState] = {}
        self._lock = Lock()
        self._stop_event = Event()
        self._thread: Thread | None = None
        self._idle_timer: Timer | None = None
        self._sequence = 0
        self._connected = False
        self._last_frame_at: float | None = None
        self._last_error: str | None = None

    def add_subscription(self, state: SubscriptionState) -> None:
        with self._lock:
            self._subscriptions[state.effective.id] = state
            self.buffer.set_duration(self._required_buffer_seconds_locked())
            if self._idle_timer:
                self._idle_timer.cancel()
                self._idle_timer = None
            should_start = self._thread is None or not self._thread.is_alive()
        if should_start:
            self.start()

    def remove_subscription(self, subscription_id: str) -> None:
        with self._lock:
            self._subscriptions.pop(subscription_id, None)
            remaining = len(self._subscriptions)
            self.buffer.set_duration(self._required_buffer_seconds_locked())
        if remaining == 0:
            self._schedule_idle_stop()

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = Thread(target=self._capture_loop, name=f"camera-{self.camera.id}", daemon=True)
            self._thread.start()
        self._health_callback(self.camera.id, "info", "capture started")

    def stop(self) -> None:
        if self._idle_timer:
            self._idle_timer.cancel()
            self._idle_timer = None
        self._stop_event.set()
        self.reader.close()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=2)
        self._connected = False

    def status(self) -> dict[str, Any]:
        with self._lock:
            active_subscriptions = len(self._subscriptions)
            requested_fps = self._required_fps_locked()
        return {
            "id": self.camera.id,
            "nickname": self.camera.nickname,
            "enabled": self.camera.enabled,
            "active": self.is_active,
            "connected": self._connected,
            "activeSubscriptions": active_subscriptions,
            "requestedFps": requested_fps,
            "bufferFrames": self.buffer.frame_count,
            "lastFrameAt": self._last_frame_at,
            "lastError": self._last_error,
        }

    @property
    def is_active(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def active_subscription_count(self) -> int:
        with self._lock:
            return len(self._subscriptions)

    def _schedule_idle_stop(self) -> None:
        if self._idle_timer:
            self._idle_timer.cancel()
        self._idle_timer = Timer(self.config.idle_grace_seconds, self._stop_if_idle)
        self._idle_timer.daemon = True
        self._idle_timer.start()

    def _stop_if_idle(self) -> None:
        with self._lock:
            if self._subscriptions:
                return
        self._health_callback(self.camera.id, "info", "capture stopped after idle grace")
        self.stop()

    def _capture_loop(self) -> None:
        while not self._stop_event.is_set():
            if not self._connected:
                self._connected = self.reader.connect()
                if not self._connected:
                    self._last_error = "connect failed"
                    self._health_callback(self.camera.id, "warning", "camera connect failed")
                    self._stop_event.wait(3)
                    continue
                self._last_error = None
                self._health_callback(self.camera.id, "info", "camera connected")

            fps = self._required_fps()
            if fps <= 0:
                self._stop_event.wait(0.1)
                continue

            started = time.time()
            frame = self.reader.next_frame()
            if frame is None:
                self._connected = False
                self.reader.close()
                self._last_error = "stream ended"
                self._health_callback(self.camera.id, "warning", "stream ended; reconnecting")
                self._stop_event.wait(1)
                continue

            captured_at = time.time()
            self._sequence += 1
            self._last_frame_at = captured_at
            self.buffer.add_frame(frame, captured_at, self._sequence)
            motion_score = self.detector.score(frame)
            self._dispatch_frame(frame, captured_at, self._sequence, motion_score)

            elapsed = time.time() - started
            self._stop_event.wait(max(0.0, (1.0 / fps) - elapsed))

    def _dispatch_frame(self, frame, captured_at: float, sequence: int, motion_score: int) -> None:
        with self._lock:
            states = list(self._subscriptions.values())

        for state in states:
            effective = state.effective
            now = time.time()
            if now - state.last_sent_at < 1.0 / effective.fps:
                continue
            triggered = self._subscription_triggered(state, motion_score, now)
            if effective.motion_gate and not triggered:
                continue

            try:
                chunk = self._build_chunk(state, frame, captured_at, sequence, motion_score, triggered)
            except Exception as e:
                self._last_error = str(e)
                self._health_callback(self.camera.id, "error", "failed to build media chunk", {"subscriptionId": effective.id, "error": str(e)})
                continue

            state.last_sent_at = now
            state.sent_sequence += 1
            self._enqueue_chunk(state, chunk)

    def _subscription_triggered(self, state: SubscriptionState, motion_score: int, now: float) -> bool:
        effective = state.effective
        if motion_score < effective.motion_threshold:
            return False
        if now - state.last_trigger_at < effective.cooldown_seconds:
            return False
        state.last_trigger_at = now
        return True

    def _build_chunk(self, state: SubscriptionState, frame, captured_at: float, sequence: int, motion_score: int, triggered: bool) -> MediaChunk:
        effective = state.effective
        if effective.mode == StreamMode.STILLS:
            resized = resize_frame(frame, effective.resolution)
            payload = encode_frame_as_jpeg(resized)
            media_type = "image/jpeg"
            metadata = {
                "frameSequence": sequence,
                "motionScore": motion_score,
                "triggered": triggered,
            }
        else:
            if effective.motion_gate:
                frames = self.buffer.recent_frames(effective.clip_seconds)
                segment_kind = "rolling"
                frame_start_sequence = frames[0].sequence if frames else None
                frame_end_sequence = frames[-1].sequence if frames else None
            else:
                frames = self.buffer.frames_since(state.last_video_sequence, sequence)
                segment_kind = "continuous"
                frame_start_sequence = frames[0].sequence if frames else None
                frame_end_sequence = frames[-1].sequence if frames else None
                if frames:
                    state.last_video_sequence = frames[-1].sequence
                    state.video_segment_sequence += 1
            payload = encode_frames_as_mp4(frames, effective.resolution, effective.fps)
            media_type = "video/mp4"
            metadata = {
                "frameSequence": sequence,
                "motionScore": motion_score,
                "triggered": triggered,
                "segmentKind": segment_kind,
                "segmentSequence": state.video_segment_sequence if not effective.motion_gate else None,
                "frameStartSequence": frame_start_sequence,
                "frameEndSequence": frame_end_sequence,
                "clipSeconds": effective.clip_seconds,
            }

        return MediaChunk(
            subscription=effective,
            timestamp=captured_at,
            sequence=state.sent_sequence + 1,
            media_type=media_type,
            payload=payload,
            metadata=metadata,
        )

    def _enqueue_chunk(self, state: SubscriptionState, chunk: MediaChunk) -> None:
        def put_drop_oldest() -> None:
            if state.queue.full():
                try:
                    state.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            state.queue.put_nowait(chunk_to_message(chunk))

        state.loop.call_soon_threadsafe(put_drop_oldest)

    def _required_fps(self) -> float:
        with self._lock:
            return self._required_fps_locked()

    def _required_fps_locked(self) -> float:
        if not self._subscriptions:
            return 0.0
        return max(state.effective.fps for state in self._subscriptions.values())

    def _required_buffer_seconds_locked(self) -> float:
        if not self._subscriptions:
            return self.config.defaults.clip_seconds
        return max(state.effective.clip_seconds for state in self._subscriptions.values())


class CameraBroker:
    def __init__(self, config: AppConfig):
        self.config = config
        self._health_events = deque(maxlen=config.limits.health_log_size)
        self._runtimes = {
            camera.id: CameraRuntime(camera, config, self.record_health)
            for camera in config.cameras
            if camera.enabled
        }
        self._cameras = {camera.id: camera for camera in config.cameras}
        self._health_stop = Event()
        self._health_thread: Thread | None = None

    def start_health_polling(self) -> None:
        self._health_stop.clear()
        self._health_thread = Thread(target=self._health_loop, name="camera-health", daemon=True)
        self._health_thread.start()

    def stop(self) -> None:
        self._health_stop.set()
        if self._health_thread and self._health_thread.is_alive():
            self._health_thread.join(timeout=2)
        for runtime in self._runtimes.values():
            runtime.stop()

    def subscribe(self, raw: dict[str, Any], loop: asyncio.AbstractEventLoop) -> SubscriptionState:
        if raw.get("type") != "subscribe":
            raise ConfigError("handshake must be a subscribe message with type='subscribe'")
        camera_id = str(raw.get("cameraId", ""))
        runtime = self._runtimes.get(camera_id)
        if runtime is None:
            camera = self._cameras.get(camera_id)
            if camera and not camera.enabled:
                raise ConfigError(f"camera is disabled: {camera_id}")
            raise ConfigError(f"unknown cameraId: {camera_id}")

        if self._active_camera_count_excluding(camera_id) >= self.config.limits.max_concurrent_active_cameras and not runtime.is_active:
            raise ConfigError("max active camera limit reached")

        effective = self._effective_subscription(raw)
        queue: asyncio.Queue = asyncio.Queue(maxsize=effective.queue_size)
        state = SubscriptionState(effective=effective, queue=queue, loop=loop)
        runtime.add_subscription(state)
        self.record_health(camera_id, "info", "subscription added", {"subscriptionId": effective.id})
        return state

    def unsubscribe(self, state: SubscriptionState) -> None:
        runtime = self._runtimes.get(state.effective.camera_id)
        if runtime:
            runtime.remove_subscription(state.effective.id)
            self.record_health(state.effective.camera_id, "info", "subscription removed", {"subscriptionId": state.effective.id})

    def status(self) -> dict[str, Any]:
        active_cameras = sum(1 for runtime in self._runtimes.values() if runtime.is_active)
        active_subscriptions = sum(runtime.active_subscription_count for runtime in self._runtimes.values())
        return {
            "status": "ok",
            "activeCameras": active_cameras,
            "activeSubscriptions": active_subscriptions,
            "health": [event.as_dict() for event in list(self._health_events)],
        }

    def cameras_status(self) -> list[dict[str, Any]]:
        statuses = []
        for camera in self.config.cameras:
            runtime = self._runtimes.get(camera.id)
            if runtime:
                statuses.append(runtime.status())
            else:
                statuses.append({
                    "id": camera.id,
                    "nickname": camera.nickname,
                    "enabled": camera.enabled,
                    "active": False,
                    "connected": False,
                    "activeSubscriptions": 0,
                })
        return statuses

    def subscriptions_status(self) -> list[dict[str, Any]]:
        subscriptions = []
        for runtime in self._runtimes.values():
            with runtime._lock:
                subscriptions.extend(state.effective.as_dict() for state in runtime._subscriptions.values())
        return subscriptions

    def record_health(self, camera_id: str | None, level: str, message: str, details: dict[str, Any] | None = None) -> None:
        self._health_events.append(HealthEvent(time.time(), camera_id, level, message, details or {}))

    def _effective_subscription(self, raw: dict[str, Any]) -> EffectiveSubscription:
        defaults = self.config.defaults
        limits = self.config.limits
        try:
            mode = StreamMode(str(raw.get("mode")))
        except ValueError as e:
            raise ConfigError("mode must be 'stills' or 'video'") from e

        requested_resolution = _parse_resolution(raw.get("resolution"), defaults.resolution)
        return EffectiveSubscription(
            id=f"sub-{uuid4().hex}",
            camera_id=str(raw["cameraId"]),
            mode=mode,
            fps=min(_positive_float(raw.get("fps", defaults.fps), "fps"), limits.max_fps),
            resolution=clamp_resolution(requested_resolution, limits.max_resolution),
            motion_gate=_parse_bool(raw.get("motionGate", defaults.motion_gate)),
            motion_threshold=_nonnegative_int(raw.get("motionThreshold", defaults.motion_threshold), "motionThreshold"),
            cooldown_seconds=_positive_float(raw.get("cooldownSeconds", defaults.cooldown_seconds), "cooldownSeconds"),
            clip_seconds=min(_positive_float(raw.get("clipSeconds", defaults.clip_seconds), "clipSeconds"), limits.max_clip_seconds),
            duration_seconds=_optional_positive_float(raw.get("durationSeconds"), "durationSeconds"),
            queue_size=limits.client_queue_size,
        )

    def _active_camera_count_excluding(self, camera_id: str) -> int:
        return sum(1 for runtime_id, runtime in self._runtimes.items() if runtime_id != camera_id and runtime.is_active)

    def _health_loop(self) -> None:
        while not self._health_stop.is_set():
            for camera in self.config.cameras:
                if not camera.enabled:
                    continue
                alive = probe_stream_liveness(camera.stream_url)
                self.record_health(camera.id, "info" if alive else "warning", "liveness probe succeeded" if alive else "liveness probe failed")
            self._health_stop.wait(self.config.health_poll_seconds)


def chunk_to_message(chunk: MediaChunk) -> dict[str, Any]:
    encoded = base64.b64encode(chunk.payload).decode("ascii")
    return {
        "type": "chunk",
        "kind": "camera_media_chunk",
        "source": "camera-daemon",
        "subscriptionId": chunk.subscription.id,
        "cameraId": chunk.subscription.camera_id,
        "sequence": chunk.sequence,
        "timestamp": chunk.timestamp,
        "capturedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(chunk.timestamp)),
        "mode": chunk.subscription.mode.value,
        "modality": "image" if chunk.media_type.startswith("image/") else "video",
        "mediaType": chunk.media_type,
        "dataBase64": encoded,
        "sizeBytes": len(chunk.payload),
        "metadata": chunk.metadata,
    }


def _parse_resolution(value: Any, default: Resolution) -> Resolution:
    if value is None:
        return default
    if not isinstance(value, dict):
        raise ConfigError("resolution must be an object with width and height")
    return Resolution(
        width=_positive_int(value.get("width"), "resolution.width"),
        height=_positive_int(value.get("height"), "resolution.height"),
    )


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _positive_float(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as e:
        raise ConfigError(f"{field} must be a positive number") from e
    if parsed <= 0:
        raise ConfigError(f"{field} must be a positive number")
    return parsed


def _optional_positive_float(value: Any, field: str) -> float | None:
    if value is None:
        return None
    return _positive_float(value, field)


def _positive_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as e:
        raise ConfigError(f"{field} must be a positive integer") from e
    if parsed <= 0:
        raise ConfigError(f"{field} must be a positive integer")
    return parsed


def _nonnegative_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as e:
        raise ConfigError(f"{field} must be a non-negative integer") from e
    if parsed < 0:
        raise ConfigError(f"{field} must be a non-negative integer")
    return parsed
