"""Compares transports from the Python client.

BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.

Starts the .NET interop server once per transport and measures the same workload over each, so the
numbers differ only in how the bytes travel::

    PYTHONPATH=. python example/benchmark.py

Needs a .NET 10 SDK, or a prebuilt tests/BlackHole.InteropServer.

Unix domain sockets are measured only where CPython exposes them to asyncio, which is Linux and
macOS. On Windows the run reports TCP alone rather than pretending otherwise.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import subprocess
import sys
import time

from blackhole import BlackHoleClient, Message, MessageType

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
SERVER_PROJECT = REPO_ROOT / "tests" / "BlackHole.InteropServer"

CALLS = 5_000
WARMUP = 500
PUBLISHES = 50_000


def _start_server(args: list[str]) -> tuple[subprocess.Popen[str], str]:
    """Start the interop server on one transport and return it with its endpoint."""
    built = SERVER_PROJECT / "bin" / "Release" / "net10.0" / "BlackHole.InteropServer.exe"
    if built.exists():
        command = [str(built), *args]
    else:
        command = ["dotnet", "run", "--project", str(SERVER_PROJECT), "-c", "Release", "--", *args]

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        cwd=str(REPO_ROOT),
    )

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"the server exited with code {process.returncode} before it was ready")
        line = process.stdout.readline() if process.stdout else ""
        if line.startswith("READY "):
            return process, line[len("READY "):].strip()

    process.terminate()
    raise RuntimeError("the server did not report a port within 120 seconds")


async def _measure_latency(client: BlackHoleClient) -> tuple[float, float, float]:
    """Sequential RPC round trips, in microseconds."""
    for _ in range(WARMUP):
        await client.call_text("upper", "x")

    samples: list[float] = []
    for _ in range(CALLS):
        started = time.perf_counter()
        await client.call_text("upper", "x")
        samples.append((time.perf_counter() - started) * 1e6)

    samples.sort()

    def at(q: float) -> float:
        return samples[min(len(samples) - 1, max(0, int(q * len(samples)) - 1))]

    return at(0.50), at(0.90), at(0.99)


async def _measure_throughput(client: BlackHoleClient) -> tuple[float, float]:
    """Publishes one at a time, then batched, to show what batching is worth here."""
    payload = b"28.4"

    started = time.perf_counter()
    for _ in range(PUBLISHES):
        await client.publish("t", payload)
    individual = time.perf_counter() - started

    batch = [Message(MessageType.PUBLISH, "t", payload) for _ in range(256)]
    started = time.perf_counter()
    for _ in range(0, PUBLISHES, len(batch)):
        await client.send_batch(batch)
    batched = time.perf_counter() - started

    return PUBLISHES / individual, PUBLISHES / batched


async def _run(label: str, server_args: list[str], connect) -> None:
    process, endpoint = _start_server(server_args)
    try:
        client = await connect(endpoint)
        try:
            p50, p90, p99 = await _measure_latency(client)
            individual, batched = await _measure_throughput(client)
            print(
                f"  {label:<14} {p50:>8.1f}us {p90:>8.1f}us {p99:>8.1f}us   "
                f"{individual:>11,.0f}/s {batched:>11,.0f}/s"
            )
        finally:
            await client.close()
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover
            process.kill()


async def main() -> None:
    print("==========================================================")
    print("  BLACKHOLE MESSAGING - PYTHON TRANSPORT COMPARISON")
    print("==========================================================")
    print(f"  python       : {sys.version.split()[0]}")
    print(f"  platform     : {sys.platform}")
    print(f"  measured     : {time.strftime('%Y-%m-%d %H:%M')}")
    print(f"  workload     : {CALLS:,} RPC calls, {PUBLISHES:,} publishes")
    print()
    print("  transport            p50       p90       p99      one-by-one     batched(256)")
    print("  --------------   --------  --------  --------   -------------  -------------")

    await _run(
        "TCP loopback",
        ["--port", "0"],
        lambda endpoint: BlackHoleClient.connect("127.0.0.1", int(endpoint)),
    )

    if BlackHoleClient.unix_supported():
        socket_path = f"/tmp/bh-bench-{os.getpid()}.sock"
        await _run(
            "Unix socket",
            ["--unix", socket_path],
            lambda _: BlackHoleClient.connect_unix(socket_path, timeout=20),
        )
    else:
        print("  Unix socket      unavailable: CPython exposes no AF_UNIX to asyncio on this platform")

    print()
    print("  Named pipes and shared memory are .NET-only from here: asyncio has no named-pipe")
    print("  client, and shared memory needs a mapped segment and a dedicated polling thread.")
    print("  See docs/transports.md.")
    print()
    print("Gravicode Studios - led by Kang Fadhil")


if __name__ == "__main__":
    asyncio.run(main())
