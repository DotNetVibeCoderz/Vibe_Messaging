"""BlackHole wire format.

BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.

This module is the Python counterpart of ``FrameCodec`` in the .NET library, and it is the only
place in this package that knows the byte layout::

    +----------------+------+-------+--------------+---------------+--------+---------+
    | FrameLength(4) | Type | Flags | HeaderLen(2) | CorrelationId | Header | Payload |
    |    int32 LE    |  u8  |  u8   |  uint16 LE   |    int64 LE   | UTF-8  |  bytes  |
    +----------------+------+-------+--------------+---------------+--------+---------+
     \\__ counts every byte after itself __________________________________________/

Keeping encode and decode in one module is deliberate: the .NET v2 protocol had two copies of its
framing and they drifted apart. A second language implementation doubles that risk, so this file
mirrors the C# one field for field and the interop test suite runs against the real .NET server.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from enum import IntEnum, IntFlag
from typing import Final

__all__ = [
    "MessageType",
    "MessageFlags",
    "Message",
    "StreamDescriptor",
    "ProtocolError",
    "RpcError",
    "encode_frame",
    "decode_frame",
    "LENGTH_PREFIX_SIZE",
    "FIXED_HEADER_SIZE",
    "PREFIX_SIZE",
    "MAX_HEADER_LENGTH",
    "DEFAULT_MAX_FRAME_LENGTH",
]

LENGTH_PREFIX_SIZE: Final = 4
"""Bytes in the length prefix itself."""

FIXED_HEADER_SIZE: Final = 12
"""Bytes between the length prefix and the header text."""

PREFIX_SIZE: Final = LENGTH_PREFIX_SIZE + FIXED_HEADER_SIZE
"""Total bytes before the header text."""

MAX_HEADER_LENGTH: Final = 0xFFFF
"""Largest UTF-8 header the two-byte length field can describe."""

DEFAULT_MAX_FRAME_LENGTH: Final = 16 * 1024 * 1024
"""Default cap on a single frame, enforced while parsing."""

# One compiled struct for the fixed header: type, flags, header length, correlation id.
_FIXED = struct.Struct("<BBHq")
_LENGTH = struct.Struct("<i")


class MessageType(IntEnum):
    """What a message means on the wire. The numeric values are protocol; never reuse one."""

    NONE = 0x00

    RPC_REQUEST = 0x01
    RPC_RESPONSE = 0x02

    PUBLISH = 0x03
    SUBSCRIBE = 0x04
    ACK = 0x05
    UNSUBSCRIBE = 0x06

    STREAM_START = 0x10
    STREAM_CHUNK = 0x11
    STREAM_END = 0x12
    STREAM_ABORT = 0x13

    BATCH = 0x20

    PING = 0x30
    PONG = 0x31


class MessageFlags(IntFlag):
    """Per-message bit flags. One byte on the wire."""

    NONE = 0
    ERROR = 1 << 0
    COMPRESSED = 1 << 1
    NO_REPLY = 1 << 2


class ProtocolError(Exception):
    """Bytes on the wire cannot form a valid frame.

    Always fatal for the connection that produced it: once framing is lost there is no way to
    resynchronise.
    """


class RpcError(Exception):
    """A remote method failed, is unknown, timed out, or its connection dropped."""

    def __init__(self, method: str, message: str) -> None:
        super().__init__(message)
        self.method = method


@dataclass(slots=True)
class Message:
    """One unit crossing the wire.

    ``header`` and ``correlation_id`` are overloaded by ``type``: the header is an RPC method name,
    a topic, or a stream id, and the correlation id matches a reply to its request, indexes a stream
    chunk, or counts the messages inside a batch.
    """

    type: MessageType
    header: str = ""
    payload: bytes = b""
    correlation_id: int = 0
    flags: MessageFlags = MessageFlags.NONE

    @property
    def is_error(self) -> bool:
        """True when the peer reported a failure instead of a result."""
        return bool(self.flags & MessageFlags.ERROR)

    def text(self) -> str:
        """Decode the payload as UTF-8."""
        return self.payload.decode("utf-8")

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        flags = "" if self.flags == MessageFlags.NONE else f"[{self.flags.name}]"
        return f"<{self.type.name}{flags} {self.header!r} {len(self.payload)}B #{self.correlation_id}>"


@dataclass(slots=True)
class StreamDescriptor:
    """Metadata carried by ``STREAM_START`` so a receiver knows what is coming.

    ``total_length`` is ``-1`` when the sender does not know the size up front.
    """

    name: str = ""
    total_length: int = -1
    content_type: str = "application/octet-stream"

    UNKNOWN_LENGTH: Final = -1

    @property
    def has_length(self) -> bool:
        """True when the sender declared a size."""
        return self.total_length >= 0

    def encode(self) -> bytes:
        """Serialise to the STREAM_START payload layout."""
        name = self.name.encode("utf-8")
        content_type = self.content_type.encode("utf-8")
        return b"".join(
            (
                struct.pack("<qH", self.total_length, len(name)),
                name,
                struct.pack("<H", len(content_type)),
                content_type,
            )
        )

    @classmethod
    def decode(cls, payload: bytes) -> "StreamDescriptor":
        """Parse a STREAM_START payload. A short payload yields an unnamed descriptor."""
        if len(payload) < 10:
            return cls()

        total_length, name_length = struct.unpack_from("<qH", payload, 0)
        if len(payload) < 10 + name_length + 2:
            return cls(total_length=total_length)

        name = payload[10 : 10 + name_length].decode("utf-8")
        (content_type_length,) = struct.unpack_from("<H", payload, 10 + name_length)
        start = 12 + name_length
        content_type = (
            payload[start : start + content_type_length].decode("utf-8")
            if len(payload) >= start + content_type_length
            else ""
        )
        return cls(name=name, total_length=total_length, content_type=content_type)


def encode_frame(message: Message) -> bytes:
    """Serialise one message, length prefix included."""
    header = message.header.encode("utf-8") if message.header else b""
    if len(header) > MAX_HEADER_LENGTH:
        raise ProtocolError(f"Header is {len(header)} bytes; the limit is {MAX_HEADER_LENGTH}.")

    frame_length = FIXED_HEADER_SIZE + len(header) + len(message.payload)
    if frame_length > 0x7FFFFFFF:
        raise ProtocolError("Frame exceeds int32 addressing.")

    return b"".join(
        (
            _LENGTH.pack(frame_length),
            _FIXED.pack(int(message.type), int(message.flags), len(header), message.correlation_id),
            header,
            message.payload,
        )
    )


def decode_frame(
    buffer: bytes | bytearray | memoryview,
    offset: int = 0,
    max_frame_length: int = DEFAULT_MAX_FRAME_LENGTH,
) -> tuple[Message, int] | None:
    """Parse one frame starting at ``offset``.

    Returns ``(message, bytes_consumed)``, or ``None`` when ``buffer`` does not hold a whole frame
    yet. Raises :class:`ProtocolError` when the frame header is not self-consistent.
    """
    available = len(buffer) - offset
    if available < LENGTH_PREFIX_SIZE:
        return None

    (frame_length,) = _LENGTH.unpack_from(buffer, offset)
    if frame_length < FIXED_HEADER_SIZE:
        raise ProtocolError(
            f"Frame length {frame_length} is below the {FIXED_HEADER_SIZE}-byte minimum; "
            "the stream is out of sync."
        )
    if frame_length > max_frame_length:
        raise ProtocolError(f"Frame length {frame_length} exceeds the {max_frame_length}-byte limit.")
    if available < LENGTH_PREFIX_SIZE + frame_length:
        return None

    type_value, flags_value, header_length, correlation_id = _FIXED.unpack_from(
        buffer, offset + LENGTH_PREFIX_SIZE
    )
    if FIXED_HEADER_SIZE + header_length > frame_length:
        raise ProtocolError(
            f"Header length {header_length} does not fit in a {frame_length}-byte frame."
        )

    header_start = offset + PREFIX_SIZE
    header = (
        bytes(buffer[header_start : header_start + header_length]).decode("utf-8")
        if header_length
        else ""
    )

    payload_start = header_start + header_length
    payload_length = frame_length - FIXED_HEADER_SIZE - header_length
    payload = bytes(buffer[payload_start : payload_start + payload_length]) if payload_length else b""

    # An unknown type byte is not a framing error - a newer peer may simply know more message types
    # than this client does - so it is preserved rather than rejected.
    try:
        message_type = MessageType(type_value)
    except ValueError:
        message_type = type_value  # type: ignore[assignment]

    message = Message(
        type=message_type,
        header=header,
        payload=payload,
        correlation_id=correlation_id,
        flags=MessageFlags(flags_value),
    )
    return message, LENGTH_PREFIX_SIZE + frame_length


def topic_matches(filter_: str, topic: str) -> bool:
    """Match a topic against an MQTT-style filter.

    ``+`` matches exactly one segment; ``#`` matches the remainder and may only appear last.
    Provided so client code can filter locally with the same rules the broker uses.

    This mirrors ``TopicFilter.Matches`` in the .NET library, including one place it is stricter
    than the MQTT specification: ``sensor/#`` does **not** match the bare parent topic ``sensor``,
    because ``#`` must have at least one segment to swallow. The broker is the authority on
    delivery, so agreeing with it matters more than agreeing with the spec.
    """
    filter_parts = filter_.split("/")
    topic_parts = topic.split("/")

    for index, part in enumerate(filter_parts):
        if part == "#":
            return index < len(topic_parts)
        if index >= len(topic_parts):
            return False
        if part != "+" and part != topic_parts[index]:
            return False

    return len(filter_parts) == len(topic_parts)
