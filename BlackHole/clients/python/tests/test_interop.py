"""Interop against the real .NET server.

BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.

Every test here talks to `tests/BlackHole.InteropServer`, which is the actual library. If the Python
codec and the C# codec ever disagree by a single byte, these fail.
"""

from __future__ import annotations

import asyncio
import struct

import pytest

from blackhole import BlackHoleClient, Message, MessageType, RpcError, StreamDescriptor


class TestRpc:
    async def test_echo_returns_the_exact_bytes(self, client: BlackHoleClient) -> None:
        payload = bytes(range(256))
        assert await client.call("echo", payload) == payload

    async def test_text_round_trip(self, client: BlackHoleClient) -> None:
        assert await client.call_text("upper", "halo blackhole") == "HALO BLACKHOLE"

    async def test_non_ascii_survives_the_round_trip(self, client: BlackHoleClient) -> None:
        # echo, not upper: casing rules differ between runtimes (.NET's invariant ToUpper leaves ß
        # alone where Python expands it to SS), and what matters here is that the UTF-8 bytes
        # cross unchanged in both directions.
        original = "suhu tangki 28,4 °C — αβγ — 日本語 — 🕳"
        assert (await client.call("echo", original.encode("utf-8"))).decode("utf-8") == original

    async def test_non_ascii_headers_survive_the_round_trip(self, client: BlackHoleClient) -> None:
        # The header takes a different path from the payload: uint16 length, decoded via the
        # server's header cache.
        assert await client.call_text("upper", "halo") == "HALO"
        with pytest.raises(RpcError, match="Unknown method"):
            await client.call("suhu/tangki/°C", timeout=5.0)

    async def test_numeric_payload(self, client: BlackHoleClient) -> None:
        result = await client.call("sum", bytes([1, 2, 3, 4, 5]))
        assert struct.unpack("<i", result)[0] == 15

    async def test_many_concurrent_calls_stay_correlated(self, client: BlackHoleClient) -> None:
        results = await asyncio.gather(
            *(client.call_text("upper", f"call-{i}") for i in range(200))
        )
        assert results == [f"CALL-{i}" for i in range(200)]

    async def test_a_handler_failure_surfaces(self, client: BlackHoleClient) -> None:
        with pytest.raises(RpcError) as caught:
            await client.call("boom")
        assert caught.value.method == "boom"
        assert "boom" in str(caught.value)

    async def test_an_unknown_method_fails_fast(self, client: BlackHoleClient) -> None:
        with pytest.raises(RpcError, match="Unknown method"):
            await client.call("no-such-method", timeout=5.0)

    async def test_a_deadline_is_enforced(self, client: BlackHoleClient) -> None:
        with pytest.raises(RpcError, match="deadline"):
            await client.call_text("sleep", "30000", timeout=0.3)

    async def test_a_late_reply_does_not_break_the_next_call(self, client: BlackHoleClient) -> None:
        # The timed-out call's reply arrives after the client gave up; the connection must survive.
        with pytest.raises(RpcError):
            await client.call_text("sleep", "400", timeout=0.15)
        await asyncio.sleep(0.6)
        assert await client.call_text("upper", "still here") == "STILL HERE"

    @pytest.mark.parametrize("size", [1, 1024, 64 * 1024, 1024 * 1024])
    async def test_large_payloads_cross_intact(self, client: BlackHoleClient, size: int) -> None:
        # "big" returns arbitrary bytes, so this must stay on the binary API - a payload that is
        # not valid UTF-8 is exactly what a text wrapper would mangle.
        raw = await client.call("big", str(size).encode())
        assert len(raw) == size
        assert raw == bytes((i % 251) for i in range(size))

    async def test_the_server_can_call_back_into_the_client(self, client: BlackHoleClient) -> None:
        client.register("client/identify", lambda m: f"python-sdk:{m.text()}")
        assert await client.call_text("callback", "hello") == "python-sdk:hello"


class TestPubSub:
    async def test_publish_reaches_a_subscriber(self, interop_port: int) -> None:
        received: asyncio.Future[tuple[str, bytes]] = asyncio.get_running_loop().create_future()

        async with await BlackHoleClient.connect("127.0.0.1", interop_port) as subscriber:
            await subscriber.subscribe(
                "sensor/+/temperature",
                lambda topic, payload: received.done() or received.set_result((topic, payload)),
            )
            await asyncio.sleep(0.2)

            async with await BlackHoleClient.connect("127.0.0.1", interop_port) as publisher:
                await publisher.publish("sensor/tank-3/temperature", "28.4")
                topic, payload = await asyncio.wait_for(received, 5.0)

        assert topic == "sensor/tank-3/temperature"
        assert payload == b"28.4"

    async def test_a_non_matching_topic_is_not_delivered(self, interop_port: int) -> None:
        seen: list[str] = []

        async with await BlackHoleClient.connect("127.0.0.1", interop_port) as subscriber:
            await subscriber.subscribe("sensor/+/temperature", lambda t, p: seen.append(t))
            await asyncio.sleep(0.2)

            async with await BlackHoleClient.connect("127.0.0.1", interop_port) as publisher:
                await publisher.publish("sensor/tank-3/temperature", "28.4")
                await publisher.publish("sensor/tank-3/humidity", "62")
                await asyncio.sleep(0.5)

        assert seen == ["sensor/tank-3/temperature"]

    async def test_multi_segment_wildcard(self, interop_port: int) -> None:
        seen: list[str] = []

        async with await BlackHoleClient.connect("127.0.0.1", interop_port) as subscriber:
            await subscriber.subscribe("alarm/#", lambda t, p: seen.append(t))
            await asyncio.sleep(0.2)

            async with await BlackHoleClient.connect("127.0.0.1", interop_port) as publisher:
                await publisher.publish("alarm/floor-1/pump", "overheating")
                await publisher.publish("alarm/floor-2/valve/inlet", "stuck")
                await asyncio.sleep(0.5)

        assert sorted(seen) == ["alarm/floor-1/pump", "alarm/floor-2/valve/inlet"]

    async def test_unsubscribe_stops_delivery(self, interop_port: int) -> None:
        count = 0

        def count_it(topic: str, payload: bytes) -> None:
            nonlocal count
            count += 1

        async with await BlackHoleClient.connect("127.0.0.1", interop_port) as subscriber:
            await subscriber.subscribe("news", count_it)
            await asyncio.sleep(0.2)

            async with await BlackHoleClient.connect("127.0.0.1", interop_port) as publisher:
                await publisher.publish("news", "one")
                await asyncio.sleep(0.4)
                await subscriber.unsubscribe("news")
                await asyncio.sleep(0.2)
                await publisher.publish("news", "two")
                await asyncio.sleep(0.4)

        assert count == 1


class TestStreaming:
    async def test_a_stream_arrives_complete(self, interop_port: int) -> None:
        payload = bytes((i * 7) % 256 for i in range(512 * 1024))
        confirmed: asyncio.Future[str] = asyncio.get_running_loop().create_future()

        async with await BlackHoleClient.connect("127.0.0.1", interop_port) as client:
            await client.subscribe(
                "stream/done",
                lambda t, p: confirmed.done() or confirmed.set_result(p.decode()),
            )
            await asyncio.sleep(0.2)

            sent = await client.send_stream(
                "firmware-2026",
                payload,
                descriptor=StreamDescriptor("firmware.bin", len(payload), "application/octet-stream"),
                chunk_size=16 * 1024,
            )
            confirmation = await asyncio.wait_for(confirmed, 30.0)

        assert sent == len(payload)
        assert confirmation == f"firmware-2026:{len(payload)}"

    async def test_progress_is_reported(self, interop_port: int) -> None:
        reports: list[int] = []

        async with await BlackHoleClient.connect("127.0.0.1", interop_port) as client:
            await client.send_stream(
                "progress-check",
                bytes(256 * 1024),
                chunk_size=4096,
                progress=reports.append,
            )

        assert reports, "expected at least one progress report"
        assert reports[-1] == 256 * 1024


class TestBatching:
    async def test_batched_messages_are_routed_individually(self, interop_port: int) -> None:
        count = 300
        seen: list[str] = []
        done = asyncio.Event()

        def collect(topic: str, payload: bytes) -> None:
            seen.append(topic)
            if len(seen) == count:
                done.set()

        async with await BlackHoleClient.connect("127.0.0.1", interop_port) as subscriber:
            await subscriber.subscribe("log/#", collect)
            await asyncio.sleep(0.2)

            async with await BlackHoleClient.connect("127.0.0.1", interop_port) as publisher:
                await publisher.send_batch(
                    [
                        Message(MessageType.PUBLISH, f"log/entry/{i}", f"line {i}".encode())
                        for i in range(count)
                    ]
                )
                await asyncio.wait_for(done.wait(), 15.0)

        assert len(seen) == count
        assert seen[0] == "log/entry/0"
        assert seen[-1] == f"log/entry/{count - 1}"


class TestConnection:
    async def test_keepalive_round_trip_is_measured(self, client: BlackHoleClient) -> None:
        elapsed = await client.ping()
        # Strictly positive: a coarse clock that reports zero is the bug this guards against.
        assert 0 < elapsed < 5

    async def test_statistics_count_both_directions(self, client: BlackHoleClient) -> None:
        for _ in range(25):
            await client.call_text("upper", "abc")

        assert client.statistics.messages_sent >= 25
        assert client.statistics.messages_received >= 25
        assert client.statistics.bytes_sent > 0

    async def test_configure_runs_before_the_first_message(self, interop_port: int) -> None:
        # A handler attached inside configure cannot miss a message the server pushes on accept.
        seen: list[str] = []

        client = await BlackHoleClient.connect(
            "127.0.0.1",
            interop_port,
            configure=lambda c: c.on_publish(lambda topic, payload: seen.append(topic)),
        )
        try:
            assert await client.call_text("upper", "ready") == "READY"
        finally:
            await client.close()

    async def test_pending_calls_fail_when_the_connection_closes(self, interop_port: int) -> None:
        client = await BlackHoleClient.connect("127.0.0.1", interop_port)
        pending = asyncio.create_task(client.call_text("sleep", "30000", timeout=20))
        await asyncio.sleep(0.3)
        await client.close()

        with pytest.raises(RpcError):
            await pending
