import base64

import numpy as np

from camera_daemon import (
    FrameBuffer,
    FrameEvent,
    StreamDispatcher,
    StreamFormat,
    StreamMode,
    StreamSubscription,
    estimated_fps,
)


class CaptureSink:
    def __init__(self):
        self.chunks = []

    def handle_chunk(self, chunk, event, buffer):
        self.chunks.append(chunk)


def frame(value: int):
    return np.full((32, 32, 3), value, dtype=np.uint8)


def event(timestamp: float, sequence: int, buffer: FrameBuffer):
    return FrameEvent(
        frame=frame(sequence),
        timestamp=timestamp,
        motion=False,
        triggered=False,
        cooldown_s=5,
        buffer_frames=buffer.frame_count,
        frame_sequence=sequence,
    )


def test_frame_buffer_trims_by_time_window():
    buffer = FrameBuffer(duration_s=1, fps=10)
    buffer.add_frame(frame(1), timestamp=10.0, sequence=1)
    buffer.add_frame(frame(2), timestamp=10.5, sequence=2)
    buffer.add_frame(frame(3), timestamp=11.2, sequence=3)

    frames = buffer.frames_since(0, 99)

    assert [item.sequence for item in frames] == [2, 3]


def test_estimated_fps_uses_buffer_timestamps():
    buffer = FrameBuffer(duration_s=10, fps=2)
    buffer.add_frame(frame(1), timestamp=10.0, sequence=1)
    buffer.add_frame(frame(2), timestamp=10.5, sequence=2)
    buffer.add_frame(frame(3), timestamp=11.0, sequence=3)

    assert estimated_fps(buffer.frames_since(0, 99), fallback=2) == 2


def test_continuous_video_segments_do_not_overlap():
    buffer = FrameBuffer(duration_s=10, fps=2)
    dispatcher = StreamDispatcher(buffer)
    sink = CaptureSink()
    dispatcher.add_sink(sink)
    dispatcher.add_subscription(
        StreamSubscription(
            name="video",
            mode=StreamMode.VIDEO,
            fps=1,
            motion_gate=False,
            format=StreamFormat.BASE64,
        )
    )

    for sequence in (1, 2, 3):
        buffer.add_frame(frame(sequence), timestamp=float(sequence), sequence=sequence)
    dispatcher.dispatch(event(3.0, 3, buffer))

    for sequence in (4, 5):
        buffer.add_frame(frame(sequence), timestamp=float(sequence), sequence=sequence)
    dispatcher.dispatch(event(5.0, 5, buffer))

    assert len(sink.chunks) == 2
    assert sink.chunks[0].metadata["segment_kind"] == "continuous"
    assert sink.chunks[0].metadata["segment_sequence"] == 1
    assert sink.chunks[0].metadata["frame_start_sequence"] == 1
    assert sink.chunks[0].metadata["frame_end_sequence"] == 3
    assert sink.chunks[1].metadata["segment_sequence"] == 2
    assert sink.chunks[1].metadata["frame_start_sequence"] == 4
    assert sink.chunks[1].metadata["frame_end_sequence"] == 5
    assert base64.b64decode(sink.chunks[0].payload).find(b"ftyp") >= 0


def test_motion_gated_video_keeps_rolling_clip_semantics():
    buffer = FrameBuffer(duration_s=10, fps=2)
    dispatcher = StreamDispatcher(buffer)
    sink = CaptureSink()
    dispatcher.add_sink(sink)
    dispatcher.add_subscription(
        StreamSubscription(
            name="motion-video",
            mode=StreamMode.VIDEO,
            fps=1,
            motion_gate=True,
            format=StreamFormat.BASE64,
        )
    )

    for sequence in (1, 2, 3):
        buffer.add_frame(frame(sequence), timestamp=float(sequence), sequence=sequence)

    dispatcher.dispatch(event(3.0, 3, buffer))
    assert len(sink.chunks) == 0

    triggered = FrameEvent(
        frame=frame(3),
        timestamp=4.0,
        motion=True,
        triggered=True,
        cooldown_s=5,
        buffer_frames=buffer.frame_count,
        frame_sequence=3,
    )
    dispatcher.dispatch(triggered)

    assert len(sink.chunks) == 1
    assert sink.chunks[0].metadata["segment_kind"] == "rolling"
    assert sink.chunks[0].metadata["frame_start_sequence"] is None
    assert sink.chunks[0].metadata["frame_end_sequence"] == 3
