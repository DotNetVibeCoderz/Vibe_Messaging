"""SocketSignal - Python client.

Built by Gravicode Studios, led by Kang Fadhil.

Speaks the same small JSON protocol as the .NET client: a client can call server methods and
get return values back, and the server can call methods registered here.

    import asyncio
    from socketsignal import SocketSignalClient

    async def main():
        client = SocketSignalClient()

        @client.on("serverHello")
        async def hello(text):
            print("server said", text)
            return "python heard you"

        await client.connect("ws://localhost:8080/ws/")
        print(await client.call("sum", 5, 7))
        await client.close()

    asyncio.run(main())

Requires the `websockets` package (``pip install websockets``).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
from typing import Any, Awaitable, Callable

import websockets

__all__ = [
    "SocketSignalClient",
    "SocketSignalError",
    "SignalInvocationError",
    "SignalTimeoutError",
    "SignalClosedError",
]

__version__ = "2.0.0"

_log = logging.getLogger("socketsignal")

Handler = Callable[..., Any | Awaitable[Any]]


class SocketSignalError(Exception):
    """Base class for every failure this client raises deliberately."""


class SignalInvocationError(SocketSignalError):
    """The remote handler ran and threw. Stack traces never cross the wire."""

    def __init__(self, method: str, remote_message: str) -> None:
        super().__init__(f"Remote method {method!r} failed: {remote_message}")
        self.method = method
        self.remote_message = remote_message


class SignalTimeoutError(SocketSignalError):
    """The reply did not arrive inside ``call_timeout``."""

    def __init__(self, method: str, timeout: float) -> None:
        super().__init__(f"Remote method {method!r} did not answer within {timeout}s.")
        self.method = method
        self.timeout = timeout


class SignalClosedError(SocketSignalError):
    """The socket went away with calls still in flight - they fail rather than hang."""


class SocketSignalClient:
    """A SocketSignal client.

    :param call_timeout: seconds to wait for a reply; ``None`` waits forever.
    :param keep_alive: seconds between protocol pings; ``None`` disables them.
    :param auto_reconnect: reconnect with exponential backoff when the socket drops.
    """

    def __init__(
        self,
        *,
        call_timeout: float | None = 30.0,
        keep_alive: float | None = 15.0,
        auto_reconnect: bool = False,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 30.0,
    ) -> None:
        self.call_timeout = call_timeout
        self.keep_alive = keep_alive
        self.auto_reconnect = auto_reconnect
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay

        self.client_id: str | None = None
        self.on_connected: Callable[[str], None] | None = None
        self.on_disconnected: Callable[[str], None] | None = None

        self._url: str | None = None
        self._socket: Any = None
        self._handlers: dict[str, Handler] = {}
        self._pending: dict[str, asyncio.Future] = {}
        self._next_id = 0
        self._pump: asyncio.Task | None = None
        self._pinger: asyncio.Task | None = None
        self._closing = False
        self._welcomed: asyncio.Future | None = None

    # ------------------------------------------------------------------ registration

    def on(self, method: str, handler: Handler | None = None):
        """Register a method the server may call. Usable as a decorator.

        The handler may be sync or async. What it returns becomes the reply when the server
        asked for one; raising sends the exception message back as an error instead.
        """
        if handler is not None:
            self._handlers[method] = handler
            return handler

        def decorate(func: Handler) -> Handler:
            self._handlers[method] = func
            return func

        return decorate

    def off(self, method: str) -> bool:
        """Remove a registration."""
        return self._handlers.pop(method, None) is not None

    # ------------------------------------------------------------------ connection

    @property
    def connected(self) -> bool:
        return self._socket is not None and self._socket.state is websockets.protocol.State.OPEN

    async def connect(self, url: str | None = None) -> str:
        """Dial the server and wait for the welcome frame. Returns the assigned client id."""
        if url is not None:
            self._url = url
        if self._url is None:
            raise ValueError("No server URL was given.")

        self._closing = False
        self._socket = await websockets.connect(self._url, ping_interval=None)

        loop = asyncio.get_running_loop()
        self._welcomed = loop.create_future()
        self._pump = asyncio.create_task(self._receive_loop())

        try:
            self.client_id = await asyncio.wait_for(self._welcomed, timeout=self.call_timeout or 30.0)
        except asyncio.TimeoutError as error:
            raise SignalClosedError("the server did not send a welcome frame") from error

        if self.keep_alive:
            self._pinger = asyncio.create_task(self._keep_alive_loop())
        if self.on_connected:
            self.on_connected(self.client_id)
        return self.client_id

    async def close(self) -> None:
        """Close the connection and stop reconnecting."""
        self._closing = True
        self.auto_reconnect = False
        for task in (self._pinger, self._pump):
            if task:
                task.cancel()
        if self._socket is not None:
            await self._socket.close()
        self._fail_pending("closed by client")

    async def __aenter__(self) -> "SocketSignalClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # ------------------------------------------------------------------ calls

    async def call(self, method: str, *args: Any) -> Any:
        """Call a server method and wait for its return value."""
        if not self.connected:
            raise SignalClosedError("the client is not connected")

        call_id = self._mint_id()
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[call_id] = future

        await self._socket.send(json.dumps({
            "type": "invoke",
            "id": call_id,
            "method": method,
            "args": list(args),
            "expectReturn": True,
        }))

        try:
            if self.call_timeout is None:
                return await future
            return await asyncio.wait_for(future, timeout=self.call_timeout)
        except asyncio.TimeoutError as error:
            self._pending.pop(call_id, None)
            raise SignalTimeoutError(method, self.call_timeout or 0) from error

    async def send(self, method: str, *args: Any) -> None:
        """Call a server method without waiting for a reply."""
        if not self.connected:
            raise SignalClosedError("the client is not connected")

        await self._socket.send(json.dumps({
            "type": "invoke",
            "id": self._mint_id(),
            "method": method,
            "args": list(args),
            "expectReturn": False,
        }))

    # ------------------------------------------------------------------ pump

    async def _receive_loop(self) -> None:
        reason = "closed by peer"
        try:
            async for raw in self._socket:
                try:
                    frame = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                if isinstance(frame, dict):
                    await self._dispatch(frame)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - any transport failure ends the connection
            reason = str(error)
        finally:
            self._fail_pending(reason)
            if self.on_disconnected:
                self.on_disconnected(reason)
            if self.auto_reconnect and not self._closing:
                asyncio.create_task(self._reconnect_loop())

    async def _dispatch(self, frame: dict) -> None:
        kind = frame.get("type")

        if kind == "welcome":
            if self._welcomed is not None and not self._welcomed.done():
                self._welcomed.set_result(frame.get("id", ""))
            return

        if kind == "invoke":
            await self._invoke(frame)
            return

        if kind == "result":
            future = self._pending.pop(str(frame.get("id")), None)
            if future is None or future.done():
                return
            if frame.get("error"):
                method = frame.get("method", "call")
                future.set_exception(SignalInvocationError(method, frame["error"]))
            else:
                future.set_result(frame.get("result"))
            return

        if kind == "ping":
            await self._socket.send(json.dumps({"type": "pong", "id": frame.get("id")}))

    async def _invoke(self, frame: dict) -> None:
        method = frame.get("method", "")
        expects = bool(frame.get("expectReturn"))
        handler = self._handlers.get(method)

        if handler is None:
            if expects:
                await self._reply(frame.get("id"), error=f"Method '{method}' not found")
            return

        try:
            result = handler(*(frame.get("args") or []))
            if inspect.isawaitable(result):
                result = await result
            if expects:
                await self._reply(frame.get("id"), result=result)
        except Exception as error:  # noqa: BLE001 - the message goes back to the caller
            _log.debug("handler %s failed", method, exc_info=True)
            if expects:
                await self._reply(frame.get("id"), error=str(error))

    async def _reply(self, call_id: Any, *, result: Any = None, error: str | None = None) -> None:
        if not self.connected:
            return
        payload = {"type": "result", "id": call_id}
        if error is None:
            payload["result"] = result
        else:
            payload["error"] = error
        await self._socket.send(json.dumps(payload))

    async def _keep_alive_loop(self) -> None:
        try:
            while self.connected:
                await asyncio.sleep(self.keep_alive)
                if self.connected:
                    await self._socket.send(json.dumps({"type": "ping", "id": self._mint_id()}))
        except (asyncio.CancelledError, Exception):  # noqa: B014 - either way the pump reports it
            return

    async def _reconnect_loop(self) -> None:
        delay = self.reconnect_delay
        while not self._closing:
            await asyncio.sleep(delay)
            try:
                await self.connect()
                return
            except Exception:  # noqa: BLE001 - keep trying until told to stop
                delay = min(delay * 2, self.max_reconnect_delay)

    def _fail_pending(self, reason: str) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(SignalClosedError(reason))
        self._pending.clear()

    def _mint_id(self) -> str:
        self._next_id += 1
        return str(self._next_id)
