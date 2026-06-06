#!/usr/bin/env python3
"""
Camera Daemon — ESP32-CAM video pipeline for Watch.

Connects to the ESP32-CAM MJPEG stream, decodes frames,
performs motion detection, and outputs events for Watch consumption.

MVP: Single hardcoded stream, motion-gated stills output.
Phase 2: WebSocket server for multi-client configurable streams.
"""

import argparse
import json
import logging
import os
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from threading import Lock, Thread
from urllib.parse import urlparse

import cv2
import numpy as np

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
DEFAULT_COOLDOWN_S = 5  # seconds between motion triggers
DEFAULT_MOTION_THRESHOLD = 5000  # pixel diff threshold
DEFAULT_HTTP_PORT = 8081  # for serving latest frame
DEFAULT_VIDEO_BUFFER_S = 3  # seconds of rolling video buffer

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
        self._max_frames = int(duration_s * fps)
        self._frames: list[tuple[float, np.ndarray]] = []
        self._lock = Lock()

    def add_frame(self, frame: np.ndarray):
        """Add a frame to the rolling buffer."""
        with self._lock:
            self._frames.append((time.time(), frame))
            # Trim to max size
            if len(self._frames) > self._max_frames:
                self._frames = self._frames[-self._max_frames:]

    def dump_to_video(self, output_path: Path) -> bool:
        """Write the current buffer to an MP4 file. Returns True on success."""
        with self._lock:
            frames = list(self._frames)

        if not frames:
            log.warning("No frames in buffer to write")
            return False

        h, w = frames[0][1].shape[:2]
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(str(output_path), fourcc, self.fps, (w, h))

        for _, frame in frames:
            writer.write(frame)

        writer.release()
        log.info(f"Wrote {len(frames)} frames to {output_path}")
        return True

    @property
    def frame_count(self) -> int:
        with self._lock:
            return len(self._frames)


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
        cooldown_s: int = DEFAULT_COOLDOWN_S,
        motion_threshold: int = DEFAULT_MOTION_THRESHOLD,
        http_port: int = DEFAULT_HTTP_PORT,
        video_buffer_s: float = DEFAULT_VIDEO_BUFFER_S,
    ):
        self.camera_url = camera_url
        self.analysis_fps = analysis_fps
        self.cooldown_s = cooldown_s
        self.motion_threshold = motion_threshold
        self.http_port = http_port
        self.running = False

        self.reader = MJPEGStreamReader(camera_url)
        self.detector = MotionDetector(threshold=motion_threshold)
        self.buffer = FrameBuffer(duration_s=video_buffer_s, fps=analysis_fps)
        self.output = OutputManager()

        # Wire up HTTP handler
        SnapshotHandler.output_manager = self.output

    def start_http(self):
        """Start the HTTP server for serving snapshots."""
        server = HTTPServer(("127.0.0.1", self.http_port), SnapshotHandler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        log.info(f"HTTP server started on http://127.0.0.1:{self.http_port}")

    def _frame_generator(self):
        """Generate decoded frames from the MJPEG stream at analysis FPS."""
        frame_interval = 1.0 / self.analysis_fps
        last_frame_time = 0

        while self.running:
            frame = self.reader.next_frame()
            if frame is None:
                log.warning("Stream ended, attempting reconnect...")
                if not self.reader.connect():
                    time.sleep(3)
                    continue
                continue

            now = time.time()
            if now - last_frame_time < frame_interval:
                continue
            last_frame_time = now

            log.debug(f"Frame received: {frame.shape}")
            yield frame

    def run(self):
        """Main daemon loop."""
        self.running = True
        self.start_http()

        if not self.reader.connect():
            log.error("Could not connect to camera. Exiting.")
            return

        log.info("Camera daemon running. Waiting for motion...")
        last_trigger_time = 0

        for frame in self._frame_generator():
            # Add every frame to the rolling buffer
            self.buffer.add_frame(frame)

            motion, _ = self.detector.detect(frame)
            now = time.time()

            if motion and (now - last_trigger_time) > self.cooldown_s:
                last_trigger_time = now
                # Save snapshot
                filename = self.output.save_snapshot(frame)
                # Save video clip from buffer
                timestamp = int(time.time())
                video_path = Path(VIDEO_DIR) / f"clip_{timestamp}.mp4"
                self.buffer.dump_to_video(video_path)
                self.output.save_video_clip(video_path)
                self.output.log_event(
                    "motion_detected",
                    filename=filename,
                    details={"cooldown": self.cooldown_s, "video_clip": video_path.name, "buffer_frames": self.buffer.frame_count},
                )

    def stop(self):
        self.running = False
        self.reader.close()


# ─── Entry Point ─────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="ESP32-CAM Camera Daemon")
    parser.add_argument("--url", default=DEFAULT_CAMERA_URL, help="MJPEG stream URL")
    parser.add_argument("--fps", type=int, default=DEFAULT_ANALYSIS_FPS, help="Analysis frame rate")
    parser.add_argument("--cooldown", type=int, default=DEFAULT_COOLDOWN_S, help="Motion cooldown (seconds)")
    parser.add_argument("--threshold", type=int, default=DEFAULT_MOTION_THRESHOLD, help="Motion sensitivity threshold")
    parser.add_argument("--port", type=int, default=DEFAULT_HTTP_PORT, help="HTTP server port")
    parser.add_argument("--buffer", type=float, default=DEFAULT_VIDEO_BUFFER_S, help="Rolling video buffer duration (seconds)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    daemon = CameraDaemon(
        camera_url=args.url,
        analysis_fps=args.fps,
        cooldown_s=args.cooldown,
        motion_threshold=args.threshold,
        http_port=args.port,
        video_buffer_s=args.buffer,
    )

    try:
        daemon.run()
    except KeyboardInterrupt:
        log.info("Shutting down...")
        daemon.stop()


if __name__ == "__main__":
    main()
