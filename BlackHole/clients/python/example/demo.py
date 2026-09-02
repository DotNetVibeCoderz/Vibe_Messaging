"""Exercises every BlackHole pattern from Python against a running server.

BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.

Start a server first, for instance::

    dotnet run --project tests/BlackHole.InteropServer -- --port 5000

then::

    python example/demo.py --port 5000
"""

from __future__ import annotations

import argparse
import asyncio
import time

from blackhole import BlackHoleClient, Message, MessageType, RpcError, StreamDescriptor


async def main() -> None:
    parser = argparse.ArgumentParser(description="BlackHole Python client demo")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()

    def configure(client: BlackHoleClient) -> None:
        # Registered before the read loop starts, so a server that calls back the instant it
        # accepts cannot beat this registration.
        client.register("client/identify", lambda m: f"python-example:{m.text()}")

    client = await BlackHoleClient.connect_with_retry(
        args.host, args.port, attempts=5, configure=configure
    )
    print(f"connected to {args.host}:{args.port}")

    async with client:
        # --- RPC -----------------------------------------------------------
        shouted = await client.call_text("upper", "halo blackhole")
        print(f'rpc        : upper("halo blackhole") -> "{shouted}"')

        try:
            await client.call("does-not-exist", timeout=2)
        except RpcError as error:
            print(f"rpc error  : {error}")

        # --- Pub/Sub -------------------------------------------------------
        delivered: asyncio.Queue[str] = asyncio.Queue()
        await client.subscribe(
            "sensor/+/temperature",
            lambda topic, payload: delivered.put_nowait(f"{topic} = {payload.decode()}"),
        )
        await asyncio.sleep(0.3)

        await client.publish("sensor/tank-3/temperature", "28.4")
        await client.publish("sensor/tank-3/humidity", "62")  # matches no filter

        try:
            print("pubsub     :", await asyncio.wait_for(delivered.get(), 3))
        except asyncio.TimeoutError:
            print("pubsub     : nothing arrived")

        # --- Streaming -----------------------------------------------------
        payload = b"blackhole" * (128 * 1024)  # about 1.1 MiB
        started = time.perf_counter()
        sent = await client.send_stream(
            "example-upload",
            payload,
            descriptor=StreamDescriptor("example.bin", len(payload)),
            chunk_size=16 * 1024,
        )
        elapsed = time.perf_counter() - started
        print(f"streaming  : {sent / (1024 * 1024):.1f} MiB in {elapsed * 1000:.0f} ms")

        # --- Batching ------------------------------------------------------
        batch = [
            Message(MessageType.PUBLISH, "log/entry", f"line {i}".encode())
            for i in range(1000)
        ]
        started = time.perf_counter()
        await client.send_batch(batch)
        elapsed = time.perf_counter() - started
        print(f"batching   : {len(batch)} messages in one write, {elapsed * 1000:.1f} ms")

        # --- Server calling back into this client --------------------------
        try:
            answer = await client.call_text("callback", "hello")
            print(f'callback   : server asked, client answered "{answer}"')
        except RpcError as error:
            print(f"callback   : {error}")

        # --- Connection ----------------------------------------------------
        print(f"keepalive  : {await client.ping() * 1000:.3f} ms round trip")
        print(f"statistics : {client.statistics}")

    print()
    print("Gravicode Studios - led by Kang Fadhil")


if __name__ == "__main__":
    asyncio.run(main())
