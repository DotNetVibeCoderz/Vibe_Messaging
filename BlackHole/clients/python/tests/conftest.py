"""Shared fixtures: starts the real .NET interop server for the whole session.

BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.

These tests deliberately do not mock the peer. A second implementation of a wire format is only
correct if it agrees with the first one, so every assertion here runs against the actual .NET
library over a real socket.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import time

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
SERVER_PROJECT = REPO_ROOT / "tests" / "BlackHole.InteropServer"


def _server_binary() -> pathlib.Path | None:
    """The built interop server, if it has been published or built already."""
    for configuration in ("Release", "Debug"):
        candidate = (
            SERVER_PROJECT / "bin" / configuration / "net10.0" / "BlackHole.InteropServer.exe"
        )
        if candidate.exists():
            return candidate
        candidate = candidate.with_suffix("")
        if candidate.exists():
            return candidate
    return None


@pytest.fixture(scope="session")
def interop_port() -> int:
    """Start the .NET interop server on a free port and yield that port."""
    binary = _server_binary()
    if binary is not None:
        command = [str(binary), "--port", "0"]
    else:
        dotnet = os.environ.get("DOTNET_ROOT_EXE", "dotnet")
        command = [dotnet, "run", "--project", str(SERVER_PROJECT), "-c", "Release", "--", "--port", "0"]

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        cwd=str(REPO_ROOT),
    )

    port: int | None = None
    deadline = time.monotonic() + 120
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(
                    f"The interop server exited with code {process.returncode} before it was ready."
                )
            line = process.stdout.readline() if process.stdout else ""
            if line.startswith("READY "):
                port = int(line.split()[1])
                break
        if port is None:
            raise RuntimeError("The interop server did not report a port within 120 seconds.")

        yield port
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover
            process.kill()


@pytest.fixture
async def client(interop_port: int):
    """A connected client, closed when the test ends."""
    from blackhole import connect

    connection = await connect("127.0.0.1", interop_port, default_timeout=10.0)
    try:
        yield connection
    finally:
        await connection.close()
