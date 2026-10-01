"""Implementation of the BigPipe Python SDK (HTTP gateway + admin API)."""

from __future__ import annotations

import asyncio
import base64
import enum
import json
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Sequence, Union

import httpx

Value = Union[bytes, str, dict, list, int, float, bool, None]


class Acks(enum.Enum):
    """Accepted for API parity with the Kafka-protocol SDKs. The HTTP gateway always
    acknowledges after the broker has appended the batch."""

    NONE = 0
    LEADER = 1
    ALL = -1


class BigPipeError(Exception):
    """Error returned by BigPipe (``code`` is the gateway error code, e.g. ``unknown_topic``)."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message


def _http_url(url: Optional[str], bootstrap: Optional[str], default_port: int = 8082, env: str = "BIGPIPE_HTTP") -> str:
    if url:
        return url.rstrip("/")
    if bootstrap:
        # Kafka bootstrap "host:9092" → gateway on the same host.
        host = bootstrap.split(",")[0].strip().rsplit(":", 1)[0]
        return f"http://{host}:{default_port}"
    return os.environ.get(env, f"http://localhost:{default_port}").rstrip("/")


def _decode(value: Optional[str], encoding: str) -> Optional[bytes]:
    if value is None:
        return None
    return base64.b64decode(value) if encoding == "base64" else value.encode("utf-8")


@dataclass
class Record:
    """A consumed record. ``key`` and ``value`` are bytes; use ``value_str``/``json()`` for text."""

    topic: str
    partition: int
    offset: int
    timestamp: int
    key: Optional[bytes]
    value: Optional[bytes]
    headers: Dict[str, Optional[str]] = field(default_factory=dict)
    delivery_count: Optional[int] = None

    @property
    def key_str(self) -> Optional[str]:
        return None if self.key is None else self.key.decode("utf-8", "replace")

    @property
    def value_str(self) -> Optional[str]:
        return None if self.value is None else self.value.decode("utf-8", "replace")

    def json(self) -> Any:
        """Parses the value as JSON."""
        return None if self.value is None else json.loads(self.value)

    @classmethod
    def _from_json(cls, d: Dict[str, Any]) -> "Record":
        return cls(
            topic=d["topic"],
            partition=d["partition"],
            offset=d["offset"],
            timestamp=d["timestamp"],
            key=_decode(d.get("key"), d.get("key_encoding", "utf8")),
            value=_decode(d.get("value"), d.get("value_encoding", "utf8")),
            headers=d.get("headers") or {},
            delivery_count=d.get("delivery_count"),
        )


@dataclass(frozen=True)
class RecordMetadata:
    topic: str
    partition: int
    offset: int


class _Http:
    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout: float = 60.0):
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.client = httpx.AsyncClient(base_url=base_url, timeout=httpx.Timeout(timeout, connect=10.0), headers=headers)

    async def call(self, method: str, path: str, *, body: Any = None, params: Optional[Dict[str, Any]] = None,
                   content: Optional[str] = None, content_type: Optional[str] = None) -> Any:
        kwargs: Dict[str, Any] = {"params": {k: v for k, v in (params or {}).items() if v is not None}}
        if content is not None:
            kwargs["content"] = content
            kwargs["headers"] = {"content-type": content_type or "text/plain"}
        elif body is not None:
            kwargs["json"] = body
        resp = await self.client.request(method, path, **kwargs)
        if resp.status_code >= 400:
            try:
                err = resp.json().get("error", {})
                raise BigPipeError(resp.status_code, err.get("code", "error"), err.get("message", resp.text))
            except ValueError:
                raise BigPipeError(resp.status_code, "error", resp.text) from None
        return resp.json() if resp.content else None

    async def close(self) -> None:
        await self.client.aclose()


def _encode_value(value: Value) -> Dict[str, Any]:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"value": base64.b64encode(bytes(value)).decode(), "value_encoding": "base64"}
    return {"value": value}


def _encode_key(key: Union[bytes, str, None]) -> Dict[str, Any]:
    if key is None:
        return {}
    if isinstance(key, (bytes, bytearray)):
        return {"key": base64.b64encode(bytes(key)).decode(), "key_encoding": "base64"}
    return {"key": str(key)}


# ----------------------------------------------------------------------------------------------
# Producer
# ----------------------------------------------------------------------------------------------


class Producer:
    """Batching producer.

    Records sent within ``linger_ms`` to the same topic travel in one HTTP request, so
    ``await asyncio.gather(*[p.send(...) for ...])`` is fast.

    >>> async with Producer(bootstrap="localhost:9092") as p:
    ...     meta = await p.send("orders", key="order-1", value={"id": 1, "amount": 150000})
    """

    def __init__(self, url: Optional[str] = None, *, bootstrap: Optional[str] = None, acks: Acks = Acks.ALL,
                 compression: Optional[str] = None, linger_ms: int = 5, max_batch: int = 500):
        self._http = _Http(_http_url(url, bootstrap))
        self._compression = None if compression in (None, "none") else compression
        self._linger = max(0, linger_ms) / 1000.0
        self._max_batch = max(1, max_batch)
        self._pending: Dict[str, List[tuple]] = {}
        self._wake: Optional[asyncio.Event] = None
        self._task: Optional[asyncio.Task] = None
        self._closed = False
        self.acks = acks

    async def __aenter__(self) -> "Producer":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def _ensure_task(self) -> None:
        if self._task is None:
            self._wake = asyncio.Event()
            self._task = asyncio.get_running_loop().create_task(self._run())

    async def send(self, topic: str, value: Value = None, *, key: Union[bytes, str, None] = None,
                   headers: Optional[Dict[str, str]] = None, partition: Optional[int] = None,
                   timestamp: Optional[int] = None) -> RecordMetadata:
        """Queues a record and returns its metadata once the broker has written it."""
        if self._closed:
            raise RuntimeError("producer is closed")
        self._ensure_task()
        rec: Dict[str, Any] = {**_encode_key(key), **_encode_value(value)}
        if headers:
            rec["headers"] = headers
        if partition is not None:
            rec["partition"] = partition
        if timestamp is not None:
            rec["timestamp"] = timestamp
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending.setdefault(topic, []).append((rec, fut))
        if len(self._pending[topic]) >= self._max_batch or self._linger == 0:
            self._wake.set()
        return await fut

    async def send_many(self, topic: str, values: Iterable[Value], key: Union[bytes, str, None] = None) -> List[RecordMetadata]:
        return list(await asyncio.gather(*(self.send(topic, v, key=key) for v in values)))

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._linger or 0.05)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            await self._flush_once()
            if self._closed and not self._pending:
                return

    async def _flush_once(self) -> None:
        batches = []
        for topic in list(self._pending):
            items = self._pending.pop(topic)
            for i in range(0, len(items), self._max_batch):
                batches.append((topic, items[i:i + self._max_batch]))
        await asyncio.gather(*(self._post(t, b) for t, b in batches))

    async def _post(self, topic: str, batch: List[tuple]) -> None:
        try:
            body: Dict[str, Any] = {"records": [r for r, _ in batch]}
            if self._compression:
                body["compression"] = self._compression
            res = await self._http.call("POST", f"/v1/topics/{topic}/records", body=body)
            for (_, fut), o in zip(batch, res["offsets"]):
                if not fut.done():
                    fut.set_result(RecordMetadata(topic, o["partition"], o["offset"]))
        except Exception as e:  # noqa: BLE001 - delivered to every waiting caller
            for _, fut in batch:
                if not fut.done():
                    fut.set_exception(e)

    async def flush(self) -> None:
        """Sends everything queued now."""
        if self._task is not None:
            await self._flush_once()

    async def close(self) -> None:
        self._closed = True
        if self._task is not None:
            self._wake.set()
            await self._task
        await self._http.close()


# ----------------------------------------------------------------------------------------------
# Consumer (server-assigned HTTP group)
# ----------------------------------------------------------------------------------------------


class Consumer:
    """Consumer-group member. BigPipe assigns partitions server-side; members just poll.

    >>> async with Consumer(bootstrap="localhost:9092", group="analytics", topics=["orders"],
    ...                     auto_offset_reset="earliest") as c:
    ...     async for msg in c:
    ...         print(msg.key_str, msg.value_str)
    ...         await c.commit(msg)
    """

    def __init__(self, url: Optional[str] = None, *, group: str, topics: Sequence[str], bootstrap: Optional[str] = None,
                 auto_offset_reset: str = "latest", auto_commit: bool = True, filter: Optional[str] = None,
                 poll_timeout_ms: int = 1000, max_records: int = 500, session_timeout_ms: int = 30000):
        if not topics:
            raise ValueError("subscribe to at least one topic")
        self._http = _Http(_http_url(url, bootstrap))
        self.group = group
        self.topics = list(topics)
        self._reset = auto_offset_reset
        self._auto_commit = auto_commit
        self._filter = filter
        self._poll_timeout = poll_timeout_ms
        self._max_records = max_records
        self._session_timeout = session_timeout_ms
        self.member_id: Optional[str] = None
        self.assignment: List[tuple] = []

    async def __aenter__(self) -> "Consumer":
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def start(self) -> None:
        res = await self._http.call("POST", f"/v1/groups/{self.group}/members", body={
            "topics": self.topics,
            "auto_commit": self._auto_commit,
            "offset_reset": self._reset,
            "filter": self._filter,
            "session_timeout_ms": self._session_timeout,
        })
        self.member_id = res["member_id"]

    async def poll(self, timeout_ms: Optional[int] = None) -> List[Record]:
        """Returns the next records (empty after the timeout when nothing arrived)."""
        if self.member_id is None:
            await self.start()
        try:
            res = await self._http.call("GET", f"/v1/groups/{self.group}/members/{self.member_id}/records", params={
                "timeout_ms": self._poll_timeout if timeout_ms is None else timeout_ms,
                "max_records": self._max_records,
            })
        except BigPipeError as e:
            if e.status == 404:  # session expired: rejoin
                await self.start()
                return []
            raise
        self.assignment = [(a["topic"], a["partition"]) for a in res.get("assignment", [])]
        return [Record._from_json(r) for r in res["records"]]

    def __aiter__(self) -> AsyncIterator[Record]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Record]:
        while True:
            for r in await self.poll():
                yield r

    async def commit(self, record: Optional[Record] = None) -> None:
        """Commits the position after ``record`` (or every position returned so far)."""
        body = None
        if record is not None:
            body = {"offsets": [{"topic": record.topic, "partition": record.partition, "offset": record.offset + 1}]}
        await self._http.call("POST", f"/v1/groups/{self.group}/members/{self.member_id}/commit", body=body or {})

    async def close(self) -> None:
        if self.member_id is not None:
            try:
                await self._http.call("DELETE", f"/v1/groups/{self.group}/members/{self.member_id}")
            except BigPipeError:
                pass
            self.member_id = None
        await self._http.close()


# ----------------------------------------------------------------------------------------------
# Share groups
# ----------------------------------------------------------------------------------------------


class ShareRecord(Record):
    """A record locked to this worker; call ``accept()``, ``release()`` or ``reject()``."""

    _owner: "ShareConsumer"

    def accept(self) -> None:
        self._owner._ack(self, "accept")

    def release(self) -> None:
        self._owner._ack(self, "release")

    def reject(self) -> None:
        self._owner._ack(self, "reject")


class ShareConsumer:
    """Queue-style consumption: many workers share partitions, each record is acknowledged
    individually and moved to ``dlq_topic`` after ``max_attempts`` deliveries or a reject."""

    def __init__(self, url: Optional[str] = None, *, group: str, topics: Sequence[str], bootstrap: Optional[str] = None,
                 max_records: int = 100, lock_ms: int = 30000, max_attempts: int = 5, dlq_topic: Optional[str] = None,
                 filter: Optional[str] = None, member: Optional[str] = None, poll_timeout_ms: int = 1000):
        self._http = _Http(_http_url(url, bootstrap))
        self.group = group
        self.topics = list(topics)
        self.member = member or f"py-{uuid.uuid4().hex[:12]}"
        self._opts = {"max_records": max_records, "lock_ms": lock_ms, "max_attempts": max_attempts,
                      "dlq_topic": dlq_topic, "filter": filter, "timeout_ms": poll_timeout_ms}
        self._acks: List[Dict[str, Any]] = []

    async def __aenter__(self) -> "ShareConsumer":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def _ack(self, r: Record, action: str) -> None:
        self._acks.append({"topic": r.topic, "partition": r.partition, "offset": r.offset, "action": action})

    async def flush_acks(self) -> List[bool]:
        if not self._acks:
            return []
        acks, self._acks = self._acks, []
        res = await self._http.call("POST", f"/v1/share/{self.group}/ack", body={"member": self.member, "acks": acks})
        return res["results"]

    async def poll(self) -> List[ShareRecord]:
        await self.flush_acks()
        res = await self._http.call("POST", f"/v1/share/{self.group}/poll",
                                    body={"member": self.member, "topics": self.topics, **self._opts})
        out = []
        for d in res["records"]:
            base = Record._from_json(d)
            r = ShareRecord(**base.__dict__)
            r._owner = self
            out.append(r)
        return out

    def __aiter__(self) -> AsyncIterator[ShareRecord]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[ShareRecord]:
        while True:
            for r in await self.poll():
                yield r

    async def close(self) -> None:
        try:
            await self.flush_acks()
        finally:
            await self._http.close()


# ----------------------------------------------------------------------------------------------
# SSE streaming
# ----------------------------------------------------------------------------------------------


async def stream(topic: str, url: Optional[str] = None, *, bootstrap: Optional[str] = None, from_: str = "latest",
                 filter: Optional[str] = None, group: Optional[str] = None) -> AsyncIterator[Record]:
    """Streams records over Server-Sent Events. ``filter`` is a bpql expression evaluated by
    the broker, e.g. ``'header("region") == "ID" && this.amount > 1000'``."""
    params = {"from": from_, "filter": filter, "group": group}
    async with httpx.AsyncClient(base_url=_http_url(url, bootstrap), timeout=None) as client:
        async with client.stream("GET", f"/v1/topics/{topic}/stream",
                                 params={k: v for k, v in params.items() if v is not None}) as resp:
            if resp.status_code >= 400:
                body = json.loads(await resp.aread() or b"{}")
                err = body.get("error", {})
                raise BigPipeError(resp.status_code, err.get("code", "error"), err.get("message", ""))
            event = None
            async for line in resp.aiter_lines():
                if line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:") and event == "record":
                    yield Record._from_json(json.loads(line[5:]))
                elif not line:
                    event = None


# ----------------------------------------------------------------------------------------------
# Admin
# ----------------------------------------------------------------------------------------------


class Admin:
    """Admin API client: topics, storage-mode migration, groups, flows, message browser."""

    def __init__(self, url: Optional[str] = None, *, api_key: Optional[str] = None):
        base = url or os.environ.get("BIGPIPE_ADMIN", "http://localhost:9644")
        self._http = _Http(base.rstrip("/"), api_key=api_key or os.environ.get("BIGPIPE_API_KEY"))

    async def __aenter__(self) -> "Admin":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def cluster(self) -> Dict[str, Any]:
        return await self._http.call("GET", "/v1/cluster")

    async def metrics(self) -> Dict[str, Any]:
        return await self._http.call("GET", "/v1/metrics")

    async def topics(self) -> List[Dict[str, Any]]:
        return await self._http.call("GET", "/v1/topics")

    async def topic(self, name: str) -> Dict[str, Any]:
        return await self._http.call("GET", f"/v1/topics/{name}")

    async def create_topic(self, name: str, partitions: int = 3, mode: str = "local",
                           config: Optional[Dict[str, Any]] = None, exist_ok: bool = False) -> Dict[str, Any]:
        try:
            return await self._http.call("POST", "/v1/topics",
                                         body={"name": name, "partitions": partitions, "mode": mode, "config": config or {}})
        except BigPipeError as e:
            if exist_ok and e.status == 409:
                return await self.topic(name)
            raise

    async def delete_topic(self, name: str) -> None:
        await self._http.call("DELETE", f"/v1/topics/{name}")

    async def update_config(self, name: str, changes: Dict[str, Any]) -> Dict[str, Any]:
        return await self._http.call("PATCH", f"/v1/topics/{name}/config", body={"config": changes})

    async def migrate(self, name: str, to: str) -> Dict[str, Any]:
        """Moves new data of a topic to ``local``, ``tiered`` or ``diskless`` (offsets never change)."""
        return await self._http.call("POST", f"/v1/topics/{name}/migrate", body={"to": to})

    async def compact(self, name: str) -> Dict[str, Any]:
        """Compacts a ``cleanup.policy=compact`` topic now (keeps the latest record per key)."""
        return await self._http.call("POST", f"/v1/topics/{name}/compact", body={})

    async def add_partitions(self, name: str, count: int) -> Dict[str, Any]:
        return await self._http.call("POST", f"/v1/topics/{name}/partitions", body={"count": count})

    async def browse(self, topic: str, limit: int = 50, *, partition: Optional[int] = None, offset: Optional[str] = None,
                     filter: Optional[str] = None) -> List[Record]:
        res = await self._http.call("GET", f"/v1/topics/{topic}/messages",
                                    params={"limit": limit, "partition": partition, "offset": offset, "filter": filter})
        return [Record._from_json(r) for r in res["records"]]

    async def groups(self) -> List[Dict[str, Any]]:
        return await self._http.call("GET", "/v1/groups")

    async def group(self, group_id: str) -> Dict[str, Any]:
        return await self._http.call("GET", f"/v1/groups/{group_id}")

    async def reset_group(self, group_id: str, topic: str, to: str = "earliest", offset: Optional[int] = None) -> Dict[str, Any]:
        return await self._http.call("POST", f"/v1/groups/{group_id}/reset", body={"topic": topic, "to": to, "offset": offset})

    async def flows(self) -> List[Dict[str, Any]]:
        return await self._http.call("GET", "/v1/flows")

    async def deploy_flow(self, spec: Union[Dict[str, Any], str]) -> Dict[str, Any]:
        """Deploys a flow from a dict (native JSON spec) or a YAML string."""
        if isinstance(spec, str):
            return await self._http.call("POST", "/v1/flows", content=spec, content_type="application/yaml")
        return await self._http.call("POST", "/v1/flows", body=spec)

    async def delete_flow(self, name: str) -> None:
        await self._http.call("DELETE", f"/v1/flows/{name}")

    async def close(self) -> None:
        await self._http.close()
