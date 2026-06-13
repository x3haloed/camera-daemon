from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .models import (
    AppConfig,
    CameraConfig,
    DefaultSubscriptionConfig,
    LimitConfig,
    Resolution,
    ServerConfig,
)


DEFAULT_CONFIG_PATH = Path("camera_daemon.config.json")
DEFAULT_CAMERA_URL = "http://192.168.4.1:81/stream"
CAMERA_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")


class ConfigError(ValueError):
    pass


def default_config() -> AppConfig:
    return AppConfig(
        cameras=[
            CameraConfig(
                id="esp32-cam",
                nickname="ESP32-CAM",
                stream_url=DEFAULT_CAMERA_URL,
                enabled=True,
            )
        ]
    )


def load_or_create_config(path: Path = DEFAULT_CONFIG_PATH) -> AppConfig:
    if not path.exists():
        config = default_config()
        path.write_text(json.dumps(config_to_dict(config), indent=2) + "\n")
        return config

    with path.open("r", encoding="utf-8") as f:
        return config_from_dict(json.load(f))


def config_to_dict(config: AppConfig) -> dict[str, Any]:
    return {
        "server": {
            "host": config.server.host,
            "httpPort": config.server.http_port,
            "wsPort": config.server.ws_port,
        },
        "limits": {
            "maxFps": config.limits.max_fps,
            "maxResolution": config.limits.max_resolution.as_dict(),
            "maxClipSeconds": config.limits.max_clip_seconds,
            "maxConcurrentActiveCameras": config.limits.max_concurrent_active_cameras,
            "clientQueueSize": config.limits.client_queue_size,
            "healthLogSize": config.limits.health_log_size,
        },
        "defaults": {
            "fps": config.defaults.fps,
            "resolution": config.defaults.resolution.as_dict(),
            "motionGate": config.defaults.motion_gate,
            "motionThreshold": config.defaults.motion_threshold,
            "cooldownSeconds": config.defaults.cooldown_seconds,
            "clipSeconds": config.defaults.clip_seconds,
        },
        "healthPollSeconds": config.health_poll_seconds,
        "idleGraceSeconds": config.idle_grace_seconds,
        "cameras": [
            {
                "id": camera.id,
                "nickname": camera.nickname,
                "streamUrl": camera.stream_url,
                "enabled": camera.enabled,
            }
            for camera in config.cameras
        ],
    }


def config_from_dict(data: dict[str, Any]) -> AppConfig:
    server_data = data.get("server", {})
    limits_data = data.get("limits", {})
    defaults_data = data.get("defaults", {})

    cameras = [_camera_from_dict(item) for item in data.get("cameras", [])]
    ids = [camera.id for camera in cameras]
    if len(ids) != len(set(ids)):
        raise ConfigError("camera ids must be unique")
    if not cameras:
        raise ConfigError("config must define at least one camera")

    limits = LimitConfig(
        max_fps=_positive_float(limits_data.get("maxFps", 10.0), "limits.maxFps"),
        max_resolution=_resolution(limits_data.get("maxResolution", {"width": 1280, "height": 720}), "limits.maxResolution"),
        max_clip_seconds=_positive_float(limits_data.get("maxClipSeconds", 10.0), "limits.maxClipSeconds"),
        max_concurrent_active_cameras=_positive_int(limits_data.get("maxConcurrentActiveCameras", 2), "limits.maxConcurrentActiveCameras"),
        client_queue_size=_positive_int(limits_data.get("clientQueueSize", 3), "limits.clientQueueSize"),
        health_log_size=_positive_int(limits_data.get("healthLogSize", 100), "limits.healthLogSize"),
    )
    defaults = DefaultSubscriptionConfig(
        fps=_positive_float(defaults_data.get("fps", 2.0), "defaults.fps"),
        resolution=_resolution(defaults_data.get("resolution", {"width": 640, "height": 480}), "defaults.resolution"),
        motion_gate=bool(defaults_data.get("motionGate", False)),
        motion_threshold=_nonnegative_int(defaults_data.get("motionThreshold", 5000), "defaults.motionThreshold"),
        cooldown_seconds=_positive_float(defaults_data.get("cooldownSeconds", 5.0), "defaults.cooldownSeconds"),
        clip_seconds=_positive_float(defaults_data.get("clipSeconds", 3.0), "defaults.clipSeconds"),
    )

    return AppConfig(
        server=ServerConfig(
            host=str(server_data.get("host", "127.0.0.1")),
            http_port=_positive_int(server_data.get("httpPort", 8081), "server.httpPort"),
            ws_port=_positive_int(server_data.get("wsPort", 8765), "server.wsPort"),
        ),
        limits=limits,
        defaults=defaults,
        cameras=cameras,
        health_poll_seconds=_positive_float(data.get("healthPollSeconds", 10.0), "healthPollSeconds"),
        idle_grace_seconds=_positive_float(data.get("idleGraceSeconds", 2.0), "idleGraceSeconds"),
    )


def _camera_from_dict(data: dict[str, Any]) -> CameraConfig:
    camera_id = str(data.get("id", "")).strip()
    if not CAMERA_ID_RE.match(camera_id):
        raise ConfigError(f"invalid camera id: {camera_id!r}")
    stream_url = str(data.get("streamUrl", data.get("stream_url", ""))).strip()
    if not stream_url:
        raise ConfigError(f"camera {camera_id!r} must define streamUrl")
    return CameraConfig(
        id=camera_id,
        nickname=str(data.get("nickname", camera_id)),
        stream_url=stream_url,
        enabled=bool(data.get("enabled", True)),
    )


def _resolution(value: Any, field: str) -> Resolution:
    if not isinstance(value, dict):
        raise ConfigError(f"{field} must be an object with width and height")
    return Resolution(
        width=_positive_int(value.get("width"), f"{field}.width"),
        height=_positive_int(value.get("height"), f"{field}.height"),
    )


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


def _positive_float(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as e:
        raise ConfigError(f"{field} must be a positive number") from e
    if parsed <= 0:
        raise ConfigError(f"{field} must be a positive number")
    return parsed
