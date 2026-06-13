from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import numpy as np


class StreamMode(str, Enum):
    STILLS = "stills"
    VIDEO = "video"


@dataclass(frozen=True)
class Resolution:
    width: int
    height: int

    def as_dict(self) -> dict[str, int]:
        return {"width": self.width, "height": self.height}


@dataclass(frozen=True)
class CameraConfig:
    id: str
    nickname: str
    stream_url: str
    enabled: bool = True


@dataclass(frozen=True)
class ServerConfig:
    http_port: int = 8081
    ws_port: int = 8765
    host: str = "127.0.0.1"


@dataclass(frozen=True)
class LimitConfig:
    max_fps: float = 10.0
    max_resolution: Resolution = field(default_factory=lambda: Resolution(1280, 720))
    max_clip_seconds: float = 10.0
    max_concurrent_active_cameras: int = 2
    client_queue_size: int = 3
    health_log_size: int = 100


@dataclass(frozen=True)
class DefaultSubscriptionConfig:
    fps: float = 2.0
    resolution: Resolution = field(default_factory=lambda: Resolution(640, 480))
    motion_gate: bool = False
    motion_threshold: int = 5000
    cooldown_seconds: float = 5.0
    clip_seconds: float = 3.0


@dataclass(frozen=True)
class AppConfig:
    server: ServerConfig = field(default_factory=ServerConfig)
    limits: LimitConfig = field(default_factory=LimitConfig)
    defaults: DefaultSubscriptionConfig = field(default_factory=DefaultSubscriptionConfig)
    cameras: list[CameraConfig] = field(default_factory=list)
    health_poll_seconds: float = 10.0
    idle_grace_seconds: float = 2.0


@dataclass(frozen=True)
class BufferedFrame:
    captured_at: float
    sequence: int
    frame: np.ndarray


@dataclass
class EffectiveSubscription:
    id: str
    camera_id: str
    mode: StreamMode
    fps: float
    resolution: Resolution
    motion_gate: bool
    motion_threshold: int
    cooldown_seconds: float
    clip_seconds: float
    duration_seconds: float | None
    queue_size: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "subscriptionId": self.id,
            "cameraId": self.camera_id,
            "mode": self.mode.value,
            "fps": self.fps,
            "resolution": self.resolution.as_dict(),
            "motionGate": self.motion_gate,
            "motionThreshold": self.motion_threshold,
            "cooldownSeconds": self.cooldown_seconds,
            "clipSeconds": self.clip_seconds,
            "durationSeconds": self.duration_seconds,
        }


@dataclass(frozen=True)
class MediaChunk:
    subscription: EffectiveSubscription
    timestamp: float
    sequence: int
    media_type: str
    payload: bytes
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HealthEvent:
    timestamp: float
    camera_id: str | None
    level: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "cameraId": self.camera_id,
            "level": self.level,
            "message": self.message,
            "details": self.details,
        }
