"""Asyncio client for BlackHole Messaging.

BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import itertools
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence

from .protocol import (
    DEFAULT_MAX_FRAME_LENGTH,
    Message,
    MessageFlags,
    MessageType,
    ProtocolError,
    RpcError,
    StreamDescriptor,
    decode_frame,
    encode_frame,
    topic_matches,
)

__all__ = ["BlackHoleClient", "Statistics", "connect", "connect_unix"]

Handler = Callable[[Message], Any]
"""A message handler. May be a coroutine function; its result is awaited when it is."""


@dataclass(slots=True)
class Statistics:
    """Counters for one connection."""

    messages_sent: int = 0
    messages_received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    last_round_trip: float | None = None
    """Seconds for the most recent keepalive round trip, or None if none has completed."""

    def __str__(self) -> str:
        return (
            f"sent {self.messages_sent:,} msg / {self.bytes_sent:,} B, "
            f"received {self.messages_received:,} msg / {self.bytes_received:,} B"
        )


@dataclass(slots=True)
class _PendingCall:
    method: str
    future: asyncio.Future[bytes]


@dataclass(slots=True)
class _Reassembly:
    descriptor: StreamDescriptor
    buffer: io.BytesIO = field(default_factory=io.BytesIO)
    received: int = 0
    next_chunk: int = 0


class BlackHoleClient:
    """A connected BlackHole client.

    Prefer :func:`connect`, or use this as an async context manager::

        async with await BlackHoleClient.connect("127.0.0.1", 5000) as client:
            print(await client.call_text("upper", "halo"))

    The read loop runs as a background task and dispatches to registered handlers. Handlers must not
    block the event loop; hand slow work to a task or a queue.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        max_frame_length: int = DEFAULT_MAX_FRAME_LENGTH,
        default_timeout: float = 30.0,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._max_frame_length = max_frame_length

        self.default_timeout = default_timeout
        """Seconds a call waits before raising :class:`RpcError`."""

        self.statistics = Statistics()
        """Live counters for this connection."""

        self._pending: dict[int, _PendingCall] = {}
        self._correlation = itertools.count(1)
        self._methods: dict[str, Callable[[Message], Any]] = {}
        self._subscriptions: dict[str, list[Callable[[str, bytes], Any]]] = {}
        self._handlers: dict[int, list[Handler]] = {}
        self._streams: dict[str, _Reassembly] = {}
        self._stream_handlers: list[Callable[[str, StreamDescriptor, bytes], Any]] = []

        self._write_lock = asyncio.Lock()
        self._closed = asyncio.Event()
        self._close_error: Exception | None = None
        self._pong_waiters: list[asyncio.Future[None]] = []
        self._read_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------ setup

    @classmethod
    async def connect(
        cls,
        host: str = "127.0.0.1",
        port: int = 5000,
        *,
        timeout: float = 10.0,
        max_frame_length: int = DEFAULT_MAX_FRAME_LENGTH,
        default_timeout: float = 30.0,
        configure: Callable[["BlackHoleClient"], Any] | None = None,
    ) -> "BlackHoleClient":
        """Dial ``host:port`` and start receiving.

        ``configure`` runs after the client is built but *before* the read loop starts, so handlers
        registered there cannot miss a message a server pushes the instant it accepts. Registering
        after this returns is a race for that first message.
        """
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout
        )
        sock = writer.get_extra_info("socket")
        if sock is not None:
            import socket as _socket

            # BlackHole coalesces at the application layer, so letting the kernel hold small frames
            # only adds latency.
            sock.setsockopt(_socket.IPPROTO_TCP, _socket.TCP_NODELAY, 1)

        client = cls(
            reader,
            writer,
            max_frame_length=max_frame_length,
            default_timeout=default_timeout,
        )
        if configure is not None:
            result = configure(client)
            if asyncio.iscoroutine(result):
                await result

        client._read_task = asyncio.create_task(client._read_loop(), name="blackhole-read")
        return client

    @classmethod
    async def connect_unix(
        cls,
        path: str,
        *,
        timeout: float = 10.0,
        max_frame_length: int = DEFAULT_MAX_FRAME_LENGTH,
        default_timeout: float = 30.0,
        configure: Callable[["BlackHoleClient"], Any] | None = None,
    ) -> "BlackHoleClient":
        """Connect over a Unix domain socket.

        The wire format is identical to TCP; only the address family changes. What you gain is a
        shorter kernel path and a socket that is not reachable from the network at all - the file's
        permissions are the access control.

        Unix and macOS only. CPython exposes no ``AF_UNIX`` support to asyncio on Windows, so this
        raises there; use a named pipe from the .NET side or TCP on loopback instead.
        """
        if not hasattr(asyncio, "open_unix_connection"):
            raise NotImplementedError(
                "Unix domain sockets are not available to asyncio on this platform. "
                "Use connect() over loopback TCP instead."
            )

        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(path), timeout=timeout
        )

        client = cls(
            reader,
            writer,
            max_frame_length=max_frame_length,
            default_timeout=default_timeout,
        )
        if configure is not None:
            result = configure(client)
            if asyncio.iscoroutine(result):
                await result

        client._read_task = asyncio.create_task(client._read_loop(), name="blackhole-read")
        return client

    @staticmethod
    def unix_supported() -> bool:
        """True when this platform can use Unix domain sockets from asyncio."""
        return hasattr(asyncio, "open_unix_connection")

    @classmethod
    async def connect_with_retry(
        cls,
        host: str = "127.0.0.1",
        port: int = 5000,
        *,
        attempts: int = 5,
        initial_delay: float = 0.1,
        **kwargs: Any,
    ) -> "BlackHoleClient":
        """Dial with exponential backoff, for a client that may start before its server."""
        delay = initial_delay
        last: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return await cls.connect(host, port, **kwargs)
            except (OSError, asyncio.TimeoutError) as exc:
                last = exc
                if attempt == attempts:
                    break
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)
        raise ConnectionError(f"Could not connect to {host}:{port} after {attempts} attempts") from last

    # ------------------------------------------------------------------- send

    async def send(self, message: Message) -> None:
        """Write one message and flush it to the socket."""
        if self.is_closed:
            raise ConnectionError("The connection is closed.")

        frame = encode_frame(message)
        async with self._write_lock:
            self._writer.write(frame)
            await self._writer.drain()

        self.statistics.messages_sent += 1
        self.statistics.bytes_sent += len(frame)

    async def send_many(self, messages: Sequence[Message]) -> None:
        """Write several messages in one socket write.

        Cheaper than :meth:`send` per message when a burst is already in hand, and unlike
        :meth:`send_batch` the peer sees each message individually framed.
        """
        if not messages:
            return
        if self.is_closed:
            raise ConnectionError("The connection is closed.")

        frames = b"".join(encode_frame(m) for m in messages)
        async with self._write_lock:
            self._writer.write(frames)
            await self._writer.drain()

        self.statistics.messages_sent += len(messages)
        self.statistics.bytes_sent += len(frames)

    # -------------------------------------------------------------------- RPC

    async def call(
        self,
        method: str,
        payload: bytes = b"",
        *,
        timeout: float | None = None,
    ) -> bytes:
        """Call a remote method and wait for its reply.

        Raises :class:`RpcError` when the method fails, is unknown, times out, or the connection
        drops before the reply arrives.
        """
        correlation_id = next(self._correlation)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bytes] = loop.create_future()
        self._pending[correlation_id] = _PendingCall(method, future)

        try:
            await self.send(
                Message(MessageType.RPC_REQUEST, method, payload, correlation_id)
            )
        except Exception:
            self._pending.pop(correlation_id, None)
            raise

        try:
            return await asyncio.wait_for(future, timeout or self.default_timeout)
        except asyncio.TimeoutError:
            self._pending.pop(correlation_id, None)
            raise RpcError(method, f"Call to '{method}' did not complete before its deadline.") from None

    async def call_text(self, method: str, payload: str = "", *, timeout: float | None = None) -> str:
        """Text-in, text-out convenience wrapper around :meth:`call`."""
        result = await self.call(method, payload.encode("utf-8"), timeout=timeout)
        return result.decode("utf-8")

    async def notify(self, method: str, payload: bytes = b"") -> None:
        """Fire and forget: send a request and never wait for a reply."""
        await self.send(
            Message(MessageType.RPC_REQUEST, method, payload, 0, MessageFlags.NO_REPLY)
        )

    def register(self, method: str, handler: Callable[[Message], Any]) -> "BlackHoleClient":
        """Serve a method the peer may call on this client.

        The handler receives the request :class:`Message` and returns ``bytes`` or ``str``; it may
        be a coroutine function.
        """
        self._methods[method] = handler
        return self

    # ---------------------------------------------------------------- Pub/Sub

    async def subscribe(self, topic_filter: str, handler: Callable[[str, bytes], Any] | None = None) -> None:
        """Ask the broker for a topic or wildcard filter.

        ``+`` matches one segment, ``#`` matches the remainder. When ``handler`` is given it fires
        only for topics matching this filter; use :meth:`on_publish` for everything.
        """
        if handler is not None:
            self._subscriptions.setdefault(topic_filter, []).append(handler)
        await self.send(Message(MessageType.SUBSCRIBE, topic_filter))

    async def unsubscribe(self, topic_filter: str) -> None:
        """Stop receiving a filter."""
        self._subscriptions.pop(topic_filter, None)
        await self.send(Message(MessageType.UNSUBSCRIBE, topic_filter))

    async def publish(self, topic: str, payload: bytes | str) -> None:
        """Publish to a topic."""
        data = payload.encode("utf-8") if isinstance(payload, str) else payload
        await self.send(Message(MessageType.PUBLISH, topic, data))

    def on_publish(self, handler: Callable[[str, bytes], Any]) -> "BlackHoleClient":
        """Receive every delivered message, whatever its topic."""
        self.on(MessageType.PUBLISH, lambda m: handler(m.header, m.payload))
        return self

    # -------------------------------------------------------------- streaming

    async def send_stream(
        self,
        stream_id: str,
        data: bytes | io.IOBase,
        *,
        descriptor: StreamDescriptor | None = None,
        chunk_size: int = 16 * 1024,
        progress: Callable[[int], Any] | None = None,
    ) -> int:
        """Send a large body as chunks and return the bytes sent.

        ``data`` may be bytes or any binary file object. Chunks are written into the socket buffer
        and drained once per 64 KiB rather than per chunk, which is what keeps small chunk sizes
        fast.
        """
        if isinstance(data, (bytes, bytearray, memoryview)):
            source: io.IOBase = io.BytesIO(bytes(data))
            total = len(data)
        else:
            source = data
            total = -1

        meta = descriptor or StreamDescriptor(stream_id, total, "application/octet-stream")
        await self.send(Message(MessageType.STREAM_START, stream_id, meta.encode()))

        sent = 0
        index = 0
        pending = 0
        flush_threshold = 64 * 1024

        try:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break

                frame = encode_frame(Message(MessageType.STREAM_CHUNK, stream_id, chunk, index))
                async with self._write_lock:
                    self._writer.write(frame)
                    pending += len(frame)
                    if pending >= flush_threshold:
                        await self._writer.drain()
                        pending = 0

                self.statistics.messages_sent += 1
                self.statistics.bytes_sent += len(frame)
                index += 1
                sent += len(chunk)
                if progress is not None and pending == 0:
                    progress(sent)

            await self.send(Message(MessageType.STREAM_END, stream_id, b"", index))
            if progress is not None:
                progress(sent)
            return sent
        except Exception as exc:
            with contextlib.suppress(Exception):
                await self.send(
                    Message(
                        MessageType.STREAM_ABORT,
                        stream_id,
                        str(exc).encode("utf-8"),
                        0,
                        MessageFlags.ERROR,
                    )
                )
            raise

    def on_stream(self, handler: Callable[[str, StreamDescriptor, bytes], Any]) -> "BlackHoleClient":
        """Receive completed inbound streams as ``(stream_id, descriptor, data)``."""
        self._stream_handlers.append(handler)
        return self

    # --------------------------------------------------------------- batching

    async def send_batch(self, messages: Sequence[Message]) -> None:
        """Pack several messages into one frame and one socket write.

        The envelope payload is a run of complete BlackHole frames, which is exactly what the peer's
        own codec unpacks - there is no second wire format.
        """
        if not messages:
            return
        payload = b"".join(encode_frame(m) for m in messages)
        await self.send(Message(MessageType.BATCH, "", payload, len(messages)))

    # ---------------------------------------------------------------- routing

    def on(self, message_type: MessageType, handler: Handler) -> "BlackHoleClient":
        """Register a handler for one message type. Several handlers run in registration order."""
        self._handlers.setdefault(int(message_type), []).append(handler)
        return self

    # ------------------------------------------------------------- read loop

    async def _read_loop(self) -> None:
        buffer = bytearray()
        failure: Exception | None = None

        try:
            while True:
                data = await self._reader.read(64 * 1024)
                if not data:
                    break
                buffer += data

                offset = 0
                while True:
                    parsed = decode_frame(buffer, offset, self._max_frame_length)
                    if parsed is None:
                        break
                    message, consumed = parsed
                    offset += consumed
                    self.statistics.messages_received += 1
                    self.statistics.bytes_received += consumed
                    await self._dispatch(message)

                if offset:
                    del buffer[:offset]
        except asyncio.CancelledError:
            raise
        except (ProtocolError, OSError) as exc:
            failure = exc
        except Exception as exc:  # pragma: no cover - defensive
            failure = exc
        finally:
            self._close_error = failure
            self._fail_pending(failure)
            self._closed.set()

    async def _dispatch(self, message: Message) -> None:
        message_type = message.type

        if message_type == MessageType.PING:
            # Answered here so keepalive never reaches application code.
            await self.send(Message(MessageType.PONG, "", b"", message.correlation_id))
            return

        if message_type == MessageType.PONG:
            # Ping measures the elapsed time itself, so the clock reading stays on one side.
            waiters, self._pong_waiters = self._pong_waiters, []
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_result(None)
            return

        if message_type == MessageType.RPC_RESPONSE:
            self._complete_call(message)
            return

        if message_type == MessageType.RPC_REQUEST:
            await self._serve_call(message)
            return

        if message_type == MessageType.BATCH:
            await self._unpack_batch(message)
            return

        if message_type in (
            MessageType.STREAM_START,
            MessageType.STREAM_CHUNK,
            MessageType.STREAM_END,
            MessageType.STREAM_ABORT,
        ):
            await self._handle_stream(message)
            # Streams still reach type handlers, for callers that want the raw frames.

        if message_type == MessageType.PUBLISH:
            await self._deliver_publish(message)

        for handler in self._handlers.get(int(message_type), ()):
            await _maybe_await(handler(message))

    def _complete_call(self, message: Message) -> None:
        pending = self._pending.pop(message.correlation_id, None)
        if pending is None or pending.future.done():
            return  # Late reply for a call that already timed out.

        if message.is_error:
            pending.future.set_exception(RpcError(pending.method, message.text()))
        else:
            pending.future.set_result(message.payload)

    async def _serve_call(self, message: Message) -> None:
        handler = self._methods.get(message.header)
        if handler is None:
            await self.send(
                Message(
                    MessageType.RPC_RESPONSE,
                    message.header,
                    f"Unknown method '{message.header}'.".encode("utf-8"),
                    message.correlation_id,
                    MessageFlags.ERROR,
                )
            )
            return

        try:
            result = await _maybe_await(handler(message))
        except Exception as exc:
            await self.send(
                Message(
                    MessageType.RPC_RESPONSE,
                    message.header,
                    f"{type(exc).__name__}: {exc}".encode("utf-8"),
                    message.correlation_id,
                    MessageFlags.ERROR,
                )
            )
            return

        if message.flags & MessageFlags.NO_REPLY:
            return

        if result is None:
            payload = b""
        elif isinstance(result, str):
            payload = result.encode("utf-8")
        else:
            payload = bytes(result)

        await self.send(
            Message(MessageType.RPC_RESPONSE, message.header, payload, message.correlation_id)
        )

    async def _deliver_publish(self, message: Message) -> None:
        for topic_filter, handlers in self._subscriptions.items():
            if topic_matches(topic_filter, message.header):
                for handler in handlers:
                    await _maybe_await(handler(message.header, message.payload))

    async def _unpack_batch(self, message: Message) -> None:
        offset = 0
        while True:
            parsed = decode_frame(message.payload, offset, self._max_frame_length)
            if parsed is None:
                break
            inner, consumed = parsed
            offset += consumed
            # One level only: a nested envelope is a loop waiting to happen.
            if inner.type != MessageType.BATCH:
                await self._dispatch(inner)

    async def _handle_stream(self, message: Message) -> None:
        if message.type == MessageType.STREAM_START:
            self._streams[message.header] = _Reassembly(StreamDescriptor.decode(message.payload))
            return

        state = self._streams.get(message.header)
        if state is None:
            return

        if message.type == MessageType.STREAM_CHUNK:
            if message.correlation_id != state.next_chunk:
                self._streams.pop(message.header, None)
                return
            state.next_chunk += 1
            state.buffer.write(message.payload)
            state.received += len(message.payload)
            return

        if message.type == MessageType.STREAM_END:
            self._streams.pop(message.header, None)
            data = state.buffer.getvalue()
            for handler in self._stream_handlers:
                await _maybe_await(handler(message.header, state.descriptor, data))
            return

        if message.type == MessageType.STREAM_ABORT:
            self._streams.pop(message.header, None)

    def _fail_pending(self, failure: Exception | None) -> None:
        waiters, self._pong_waiters = self._pong_waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_exception(ConnectionError("The connection closed."))

        reason = str(failure) if failure else "The connection closed before the reply arrived."
        for correlation_id in list(self._pending):
            pending = self._pending.pop(correlation_id, None)
            if pending is not None and not pending.future.done():
                pending.future.set_exception(RpcError(pending.method, reason))

    # ------------------------------------------------------------- lifecycle

    @property
    def is_closed(self) -> bool:
        """True once the connection has ended."""
        return self._closed.is_set() or self._writer.is_closing()

    async def wait_closed(self) -> None:
        """Block until the connection ends."""
        await self._closed.wait()

    async def ping(self, timeout: float = 5.0) -> float:
        """Send a keepalive probe and return the round trip in seconds.

        Timed with :func:`time.perf_counter`, not :func:`time.monotonic`: on Windows the latter has
        roughly 15 ms resolution, which is coarser than a loopback round trip and reports zero.
        """
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[None] = loop.create_future()
        self._pong_waiters.append(waiter)

        started = time.perf_counter()
        await self.send(Message(MessageType.PING))

        try:
            await asyncio.wait_for(waiter, timeout)
        except asyncio.TimeoutError:
            if waiter in self._pong_waiters:
                self._pong_waiters.remove(waiter)
            raise TimeoutError("The peer did not answer the keepalive probe.") from None

        elapsed = time.perf_counter() - started
        self.statistics.last_round_trip = elapsed
        return elapsed

    async def close(self) -> None:
        """Close the connection and stop the read loop."""
        self._closed.set()

        if self._read_task is not None and not self._read_task.done():
            self._read_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._read_task

        self._fail_pending(None)

        with contextlib.suppress(Exception):
            self._writer.close()
            await self._writer.wait_closed()

    async def __aenter__(self) -> "BlackHoleClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


async def _maybe_await(result: Any) -> Any:
    """Await a handler result when it is awaitable, otherwise pass it through."""
    if asyncio.iscoroutine(result) or isinstance(result, asyncio.Future):
        return await result
    return result


async def connect(host: str = "127.0.0.1", port: int = 5000, **kwargs: Any) -> BlackHoleClient:
    """Shorthand for :meth:`BlackHoleClient.connect`."""
    return await BlackHoleClient.connect(host, port, **kwargs)


async def connect_unix(path: str, **kwargs: Any) -> BlackHoleClient:
    """Shorthand for :meth:`BlackHoleClient.connect_unix`."""
    return await BlackHoleClient.connect_unix(path, **kwargs)
