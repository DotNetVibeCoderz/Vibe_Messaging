"""BlackHole Messaging - Python client.

Gravicode Studios, led by Kang Fadhil.

A client for the BlackHole binary messaging protocol: RPC, Pub/Sub, Streaming and Batching over
TCP. Speaks the same wire format as the .NET library, verified against it by the interop suite.

    import asyncio
    from blackhole import connect

    async def main():
        async with await connect("127.0.0.1", 5000) as client:
            print(await client.call_text("upper", "halo blackhole"))

    asyncio.run(main())
"""

from .client import BlackHoleClient, Statistics, connect
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

__version__ = "3.0.0"

__all__ = [
    "BlackHoleClient",
    "Statistics",
    "connect",
    "Message",
    "MessageType",
    "MessageFlags",
    "StreamDescriptor",
    "ProtocolError",
    "RpcError",
    "encode_frame",
    "decode_frame",
    "topic_matches",
    "DEFAULT_MAX_FRAME_LENGTH",
    "__version__",
]
