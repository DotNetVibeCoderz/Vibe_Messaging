"""BigPipe Python SDK.

Async client for the BigPipe HTTP gateway: batching producer, server-assigned consumer
groups, share groups (queue semantics with ack/release/reject and dead-letter topics),
Server-Sent Events streaming with server-side bpql filters, and the admin API.

Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.
"""

from ._client import (
    Acks,
    Admin,
    BigPipeError,
    Consumer,
    Producer,
    Record,
    RecordMetadata,
    ShareConsumer,
    ShareRecord,
    stream,
)

__all__ = [
    "Acks",
    "Admin",
    "BigPipeError",
    "Consumer",
    "Producer",
    "Record",
    "RecordMetadata",
    "ShareConsumer",
    "ShareRecord",
    "stream",
]
__version__ = "0.1.0"
