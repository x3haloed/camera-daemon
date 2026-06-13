from __future__ import annotations

import logging
import socket
from urllib.parse import urlparse

import cv2

log = logging.getLogger("camera-daemon")


class MJPEGStreamReader:
    """Reads frames from an MJPEG HTTP stream using OpenCV."""

    def __init__(self, url: str):
        self.url = url
        self._cap = None

    def connect(self) -> bool:
        self.close()
        try:
            log.info("Connecting to MJPEG stream: %s", self.url)
            self._cap = cv2.VideoCapture(self.url)
            if not self._cap.isOpened():
                log.warning("OpenCV failed to open stream: %s", self.url)
                return False
            ret, frame = self._cap.read()
            if not ret or frame is None:
                log.warning("OpenCV opened stream but returned no frame: %s", self.url)
                return False
            return True
        except Exception:
            log.exception("Failed to connect to camera stream: %s", self.url)
            self.close()
            return False

    def next_frame(self):
        if self._cap is None:
            return None
        try:
            ret, frame = self._cap.read()
            if not ret or frame is None:
                return None
            return frame
        except Exception:
            log.exception("Error reading camera frame: %s", self.url)
            return None

    def close(self) -> None:
        if self._cap is not None:
            self._cap.release()
            self._cap = None


def probe_stream_liveness(url: str, timeout_s: float = 1.0) -> bool:
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return False
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False
