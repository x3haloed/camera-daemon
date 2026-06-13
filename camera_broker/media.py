from __future__ import annotations

import io
import time
from threading import Lock

import cv2
import numpy as np

from .models import BufferedFrame, Resolution


class MotionDetector:
    """Frame-differencing motion scorer."""

    def __init__(self):
        self.prev_gray = None

    def score(self, frame: np.ndarray) -> int:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (21, 21), 0)
        if self.prev_gray is None:
            self.prev_gray = gray
            return 0

        diff = cv2.absdiff(self.prev_gray, gray)
        thresh = cv2.threshold(diff, 25, 255, cv2.THRESH_BINARY)[1]
        thresh = cv2.dilate(thresh, None, iterations=2)
        self.prev_gray = gray
        return int(np.sum(thresh) // 255)


class FrameBuffer:
    """Thread-safe rolling frame buffer."""

    def __init__(self, duration_s: float, fps: float):
        self.duration_s = duration_s
        self.fps = fps
        self._frames: list[BufferedFrame] = []
        self._lock = Lock()

    def set_duration(self, duration_s: float) -> None:
        with self._lock:
            self.duration_s = duration_s
            self._trim(time.time())

    def add_frame(self, frame: np.ndarray, timestamp: float | None = None, sequence: int = 0) -> None:
        timestamp = timestamp or time.time()
        with self._lock:
            self._frames.append(BufferedFrame(timestamp, sequence, frame.copy()))
            self._trim(timestamp)

    def latest(self) -> BufferedFrame | None:
        with self._lock:
            return self._frames[-1] if self._frames else None

    def frames_since(self, after_sequence: int, through_sequence: int) -> list[BufferedFrame]:
        with self._lock:
            return [
                item
                for item in self._frames
                if after_sequence < item.sequence <= through_sequence
            ]

    def recent_frames(self, seconds: float) -> list[BufferedFrame]:
        cutoff = time.time() - seconds
        with self._lock:
            return [item for item in self._frames if item.captured_at >= cutoff]

    @property
    def frame_count(self) -> int:
        with self._lock:
            return len(self._frames)

    def _trim(self, now: float) -> None:
        cutoff = now - self.duration_s
        self._frames = [item for item in self._frames if item.captured_at >= cutoff]


def estimated_fps(frames: list[BufferedFrame], fallback: float) -> float:
    if len(frames) < 2:
        return fallback
    duration = frames[-1].captured_at - frames[0].captured_at
    if duration <= 0:
        return fallback
    return max(1.0, min(60.0, (len(frames) - 1) / duration))


def clamp_resolution(requested: Resolution, maximum: Resolution) -> Resolution:
    scale = min(maximum.width / requested.width, maximum.height / requested.height, 1.0)
    return Resolution(max(1, int(requested.width * scale)), max(1, int(requested.height * scale)))


def resize_frame(frame: np.ndarray, resolution: Resolution) -> np.ndarray:
    h, w = frame.shape[:2]
    if w == resolution.width and h == resolution.height:
        return frame
    return cv2.resize(frame, (resolution.width, resolution.height), interpolation=cv2.INTER_AREA)


def encode_frame_as_jpeg(frame: np.ndarray, quality: int = 85) -> bytes:
    ok, encoded = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    if not ok:
        raise RuntimeError("failed to encode frame as JPEG")
    return encoded.tobytes()


def encode_frames_as_mp4(frames: list[BufferedFrame], resolution: Resolution, fps: float) -> bytes:
    if not frames:
        raise RuntimeError("no frames available to encode")
    try:
        import av
    except ImportError as e:
        raise RuntimeError("PyAV is required for in-memory MP4 encoding; install requirements.txt") from e

    output = io.BytesIO()
    with av.open(output, mode="w", format="mp4") as container:
        rate = max(1, int(round(estimated_fps(frames, fps))))
        stream = container.add_stream("mpeg4", rate=rate)
        stream.width = resolution.width
        stream.height = resolution.height
        stream.pix_fmt = "yuv420p"

        for buffered in frames:
            frame = resize_frame(buffered.frame, resolution)
            video_frame = av.VideoFrame.from_ndarray(frame, format="bgr24")
            for packet in stream.encode(video_frame):
                container.mux(packet)
        for packet in stream.encode():
            container.mux(packet)

    return output.getvalue()
