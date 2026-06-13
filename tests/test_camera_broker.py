import asyncio
import base64
from urllib.request import urlopen
import json
import time

import numpy as np
import pytest

from camera_broker.config import ConfigError, config_to_dict, default_config, load_or_create_config
from camera_broker.http_server import StatusHTTPServer
from camera_broker.media import FrameBuffer, encode_frame_as_jpeg, encode_frames_as_mp4, estimated_fps
from camera_broker.models import AppConfig, BufferedFrame, CameraConfig, LimitConfig, Resolution, ServerConfig
from camera_broker.runtime import CameraBroker, chunk_to_message
from camera_broker.websocket_server import WebSocketStreamServer


def frame(value: int, size: tuple[int, int] = (32, 32)):
    return np.full((size[1], size[0], 3), value, dtype=np.uint8)


def two_camera_config() -> AppConfig:
    return AppConfig(
        server=ServerConfig(http_port=0, ws_port=0),
        limits=LimitConfig(max_fps=10, max_resolution=Resolution(1280, 720), max_clip_seconds=10, max_concurrent_active_cameras=2, client_queue_size=3),
        cameras=[
            CameraConfig(id="front", nickname="Front", stream_url="http://127.0.0.1:81/stream", enabled=True),
            CameraConfig(id="back", nickname="Back", stream_url="http://127.0.0.1:82/stream", enabled=True),
        ],
        health_poll_seconds=60,
        idle_grace_seconds=0.05,
    )


def test_missing_config_creates_default_json(tmp_path):
    path = tmp_path / "camera_daemon.config.json"

    config = load_or_create_config(path)

    assert path.exists()
    saved = json.loads(path.read_text())
    assert saved["cameras"][0]["id"] == "esp32-cam"
    assert config.cameras[0].stream_url == "http://192.168.4.1:81/stream"


def test_frame_buffer_trims_by_time_window():
    buffer = FrameBuffer(duration_s=1, fps=10)
    buffer.add_frame(frame(1), timestamp=10.0, sequence=1)
    buffer.add_frame(frame(2), timestamp=10.5, sequence=2)
    buffer.add_frame(frame(3), timestamp=11.2, sequence=3)

    frames = buffer.frames_since(0, 99)

    assert [item.sequence for item in frames] == [2, 3]


def test_estimated_fps_uses_buffer_timestamps():
    frames = [
        BufferedFrame(10.0, 1, frame(1)),
        BufferedFrame(10.5, 2, frame(2)),
        BufferedFrame(11.0, 3, frame(3)),
    ]

    assert estimated_fps(frames, fallback=2) == 2


def test_jpeg_output_is_valid():
    payload = encode_frame_as_jpeg(frame(12))

    assert payload.startswith(b"\xff\xd8")
    assert payload.endswith(b"\xff\xd9")


def test_mp4_output_is_encoded_in_memory():
    frames = [
        BufferedFrame(10.0, 1, frame(1)),
        BufferedFrame(10.5, 2, frame(2)),
        BufferedFrame(11.0, 3, frame(3)),
    ]

    payload = encode_frames_as_mp4(frames, Resolution(32, 32), fps=2)

    assert b"ftyp" in payload[:64]
    assert len(payload) > 100


def test_subscribe_rejects_old_handshake():
    broker = CameraBroker(two_camera_config())

    with pytest.raises(ConfigError, match="type='subscribe'"):
        broker.subscribe({"mode": "stills", "fps": 1}, asyncio.new_event_loop())


def test_subscribe_clamps_effective_values(monkeypatch):
    monkeypatch.setattr("camera_broker.runtime.CameraRuntime.start", lambda self: None)
    broker = CameraBroker(two_camera_config())
    loop = asyncio.new_event_loop()
    try:
        state = broker.subscribe(
            {
                "type": "subscribe",
                "cameraId": "front",
                "mode": "stills",
                "fps": 99,
                "resolution": {"width": 4000, "height": 3000},
                "clipSeconds": 99,
            },
            loop,
        )

        assert state.effective.fps == 10
        assert state.effective.resolution.width <= 1280
        assert state.effective.resolution.height <= 720
        assert state.effective.clip_seconds == 10
    finally:
        broker.unsubscribe(state)
        broker.stop()
        loop.close()


def test_disabled_camera_is_rejected():
    config = AppConfig(cameras=[CameraConfig(id="front", nickname="Front", stream_url="http://127.0.0.1:81/stream", enabled=False)])
    broker = CameraBroker(config)
    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(ConfigError, match="disabled"):
            broker.subscribe({"type": "subscribe", "cameraId": "front", "mode": "stills"}, loop)
    finally:
        broker.stop()
        loop.close()


def test_two_cameras_can_have_independent_subscriptions(monkeypatch):
    started = []

    def fake_start(self):
        started.append(self.camera.id)

    monkeypatch.setattr("camera_broker.runtime.CameraRuntime.start", fake_start)
    broker = CameraBroker(two_camera_config())
    loop = asyncio.new_event_loop()
    try:
        front = broker.subscribe({"type": "subscribe", "cameraId": "front", "mode": "stills"}, loop)
        back = broker.subscribe({"type": "subscribe", "cameraId": "back", "mode": "stills"}, loop)

        assert sorted(started) == ["back", "front"]
        assert broker.status()["activeSubscriptions"] == 2
    finally:
        broker.unsubscribe(front)
        broker.unsubscribe(back)
        broker.stop()
        loop.close()


def test_no_subscriptions_means_no_capture_runtime_is_active():
    broker = CameraBroker(two_camera_config())
    try:
        assert broker.status()["activeCameras"] == 0
        assert broker.status()["activeSubscriptions"] == 0
    finally:
        broker.stop()


def test_capture_fps_is_derived_from_highest_subscription(monkeypatch):
    monkeypatch.setattr("camera_broker.runtime.CameraRuntime.start", lambda self: None)
    broker = CameraBroker(two_camera_config())
    loop = asyncio.new_event_loop()
    try:
        first = broker.subscribe({"type": "subscribe", "cameraId": "front", "mode": "stills", "fps": 2}, loop)
        second = broker.subscribe({"type": "subscribe", "cameraId": "front", "mode": "stills", "fps": 7}, loop)
        runtime = broker._runtimes["front"]

        assert runtime._required_fps() == 7
    finally:
        broker.unsubscribe(first)
        broker.unsubscribe(second)
        broker.stop()
        loop.close()


def test_chunk_message_contains_camera_subscription_and_base64_payload(monkeypatch):
    monkeypatch.setattr("camera_broker.runtime.CameraRuntime.start", lambda self: None)
    config = default_config()
    broker = CameraBroker(config)
    loop = asyncio.new_event_loop()
    try:
        state = broker.subscribe({"type": "subscribe", "cameraId": "esp32-cam", "mode": "stills"}, loop)
        from camera_broker.models import MediaChunk

        message = chunk_to_message(MediaChunk(state.effective, time.time(), 1, "image/jpeg", b"abc", {"frameSequence": 1}))

        assert message["cameraId"] == "esp32-cam"
        assert message["subscriptionId"] == state.effective.id
        assert message["dataBase64"] == base64.b64encode(b"abc").decode("ascii")
    finally:
        broker.unsubscribe(state)
        broker.stop()
        loop.close()


def test_websocket_parse_rejects_non_object():
    broker = CameraBroker(default_config())
    server = WebSocketStreamServer(broker, "127.0.0.1", 0)

    with pytest.raises(ValueError, match="JSON object"):
        server._parse_raw("[]")


def test_config_roundtrip_has_expected_top_level_keys():
    data = config_to_dict(default_config())

    assert {"server", "limits", "defaults", "cameras"}.issubset(data)


def test_http_server_serves_dashboard_and_config():
    broker = CameraBroker(default_config())
    server = StatusHTTPServer(broker, "127.0.0.1", 0)
    try:
        server.start()
        port = server._server.server_address[1]

        with urlopen(f"http://127.0.0.1:{port}/", timeout=2) as response:
            html = response.read().decode("utf-8")
        with urlopen(f"http://127.0.0.1:{port}/config", timeout=2) as response:
            config = json.loads(response.read())

        assert "Camera Broker" in html
        assert config["config"]["cameras"][0]["id"] == "esp32-cam"
    finally:
        server.stop()
        broker.stop()
