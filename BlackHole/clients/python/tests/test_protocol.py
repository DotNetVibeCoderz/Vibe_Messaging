"""Codec tests that need no server.

BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
"""

from __future__ import annotations

import pytest

from blackhole import (
    Message,
    MessageFlags,
    MessageType,
    ProtocolError,
    StreamDescriptor,
    decode_frame,
    encode_frame,
    topic_matches,
)
from blackhole.protocol import FIXED_HEADER_SIZE, PREFIX_SIZE


def test_round_trips_every_field() -> None:
    original = Message(
        MessageType.RPC_REQUEST,
        "sensor/tank-3/temperature",
        b"28.4",
        correlation_id=987654321,
        flags=MessageFlags.NO_REPLY,
    )

    frame = encode_frame(original)
    parsed, consumed = decode_frame(frame)

    assert consumed == len(frame)
    assert parsed == original


def test_frame_layout_matches_the_specification() -> None:
    frame = encode_frame(Message(MessageType.PUBLISH, "ab", b"xyz", 7))

    # 4-byte length prefix counting everything after itself.
    assert int.from_bytes(frame[0:4], "little") == FIXED_HEADER_SIZE + 2 + 3
    assert frame[4] == int(MessageType.PUBLISH)
    assert frame[5] == 0
    assert int.from_bytes(frame[6:8], "little") == 2
    assert int.from_bytes(frame[8:16], "little", signed=True) == 7
    assert frame[PREFIX_SIZE : PREFIX_SIZE + 2] == b"ab"
    assert frame[PREFIX_SIZE + 2 :] == b"xyz"


def test_handles_empty_header_and_payload() -> None:
    parsed, _ = decode_frame(encode_frame(Message(MessageType.PING)))
    assert parsed.type is MessageType.PING
    assert parsed.header == ""
    assert parsed.payload == b""


def test_returns_none_until_the_whole_frame_arrives() -> None:
    frame = encode_frame(Message(MessageType.PUBLISH, "topic", b"body"))
    for prefix in range(len(frame)):
        assert decode_frame(frame[:prefix]) is None
    assert decode_frame(frame) is not None


def test_parses_back_to_back_frames() -> None:
    stream = b"".join(
        encode_frame(Message(MessageType.PUBLISH, f"topic/{i}", str(i).encode())) for i in range(5)
    )

    offset = 0
    seen = []
    while True:
        parsed = decode_frame(stream, offset)
        if parsed is None:
            break
        message, consumed = parsed
        offset += consumed
        seen.append(message.header)

    assert seen == [f"topic/{i}" for i in range(5)]
    assert offset == len(stream)


def test_handles_non_ascii_headers_and_payloads() -> None:
    original = Message(MessageType.PUBLISH, "suhu/tangki/derajat-°C", "28,4 °C".encode("utf-8"))
    parsed, _ = decode_frame(encode_frame(original))
    assert parsed.header == "suhu/tangki/derajat-°C"
    assert parsed.text() == "28,4 °C"


def test_rejects_a_frame_longer_than_the_limit() -> None:
    frame = encode_frame(Message(MessageType.PUBLISH, "t", b"x" * 4096))
    with pytest.raises(ProtocolError, match="exceeds"):
        decode_frame(frame, 0, max_frame_length=128)


def test_rejects_an_impossible_length_prefix() -> None:
    with pytest.raises(ProtocolError, match="out of sync"):
        decode_frame(bytes([2, 0, 0, 0, 1, 2]))


def test_rejects_an_oversized_header() -> None:
    with pytest.raises(ProtocolError, match="Header is"):
        encode_frame(Message(MessageType.PUBLISH, "x" * 70000))


def test_preserves_an_unknown_message_type() -> None:
    # A newer peer may know message types this client does not; that is not a framing error.
    frame = bytearray(encode_frame(Message(MessageType.PUBLISH, "t")))
    frame[4] = 0x7E
    parsed, _ = decode_frame(bytes(frame))
    assert int(parsed.type) == 0x7E


class TestStreamDescriptor:
    def test_round_trips(self) -> None:
        original = StreamDescriptor("kalibrasi-2026.csv", 1_048_576, "text/csv")
        assert StreamDescriptor.decode(original.encode()) == original
        assert original.has_length

    def test_decodes_an_empty_payload_as_unknown(self) -> None:
        assert StreamDescriptor.decode(b"").has_length is False

    def test_round_trips_an_unknown_length(self) -> None:
        original = StreamDescriptor("live.log", -1, "text/plain")
        assert StreamDescriptor.decode(original.encode()) == original


@pytest.mark.parametrize(
    ("filter_", "topic", "expected"),
    [
        ("sensor/tank-3/temp", "sensor/tank-3/temp", True),
        ("sensor/+/temp", "sensor/tank-3/temp", True),
        ("sensor/+/temp", "sensor/tank-3/humidity", False),
        ("sensor/+/temp", "sensor/a/b/temp", False),
        ("sensor/#", "sensor/tank-3/temp", True),
        ("sensor/#", "sensor", False),
        ("#", "anything/at/all", True),
        ("sensor/tank-3/temp", "sensor/tank-3", False),
        ("sensor/tank-3", "sensor/tank-3/temp", False),
        ("+/+/temp", "sensor/tank-3/temp", True),
    ],
)
def test_topic_matching(filter_: str, topic: str, expected: bool) -> None:
    assert topic_matches(filter_, topic) is expected
