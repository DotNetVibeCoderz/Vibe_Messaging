/**
 * BlackHole wire format.
 *
 * BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
 *
 * This module is the JavaScript counterpart of `FrameCodec` in the .NET library, and the only place
 * in this package that knows the byte layout:
 *
 * ```
 * +----------------+------+-------+--------------+---------------+--------+---------+
 * | FrameLength(4) | Type | Flags | HeaderLen(2) | CorrelationId | Header | Payload |
 * |    int32 LE    |  u8  |  u8   |  uint16 LE   |    int64 LE   | UTF-8  |  bytes  |
 * +----------------+------+-------+--------------+---------------+--------+---------+
 *  \__ counts every byte after itself ________________________________________/
 * ```
 *
 * Keeping encode and decode together is deliberate: the .NET v2 protocol kept two copies of its
 * framing and they drifted apart. The interop suite runs this package against the real .NET server
 * so a disagreement of one byte fails a test rather than a deployment.
 *
 * @module
 */

/** Bytes in the length prefix itself. */
export const LENGTH_PREFIX_SIZE = 4;

/** Bytes between the length prefix and the header text. */
export const FIXED_HEADER_SIZE = 12;

/** Total bytes before the header text. */
export const PREFIX_SIZE = LENGTH_PREFIX_SIZE + FIXED_HEADER_SIZE;

/** Largest UTF-8 header the two-byte length field can describe. */
export const MAX_HEADER_LENGTH = 0xffff;

/** Default cap on a single frame, enforced while parsing. */
export const DEFAULT_MAX_FRAME_LENGTH = 16 * 1024 * 1024;

/**
 * What a message means on the wire. The numeric values are protocol; never reuse one.
 *
 * Related types share a high nibble, so a new streaming type gets `0x14` rather than whatever
 * number happens to be free.
 *
 * @readonly
 * @enum {number}
 */
export const MessageType = Object.freeze({
  None: 0x00,

  RpcRequest: 0x01,
  RpcResponse: 0x02,

  Publish: 0x03,
  Subscribe: 0x04,
  Ack: 0x05,
  Unsubscribe: 0x06,

  StreamStart: 0x10,
  StreamChunk: 0x11,
  StreamEnd: 0x12,
  StreamAbort: 0x13,

  Batch: 0x20,

  Ping: 0x30,
  Pong: 0x31,
});

const TYPE_NAMES = Object.freeze(
  Object.fromEntries(Object.entries(MessageType).map(([name, value]) => [value, name])),
);

/**
 * The name of a message type, for logs.
 * @param {number} type
 * @returns {string}
 */
export function messageTypeName(type) {
  return TYPE_NAMES[type] ?? `Unknown(0x${type.toString(16).toUpperCase().padStart(2, '0')})`;
}

/**
 * Per-message bit flags. One byte on the wire.
 * @readonly
 * @enum {number}
 */
export const MessageFlags = Object.freeze({
  None: 0,
  Error: 1 << 0,
  Compressed: 1 << 1,
  NoReply: 1 << 2,
});

/**
 * Bytes on the wire cannot form a valid frame.
 *
 * Always fatal for the connection that produced it: once framing is lost there is no way to
 * resynchronise.
 */
export class ProtocolError extends Error {
  /** @param {string} message */
  constructor(message) {
    super(message);
    this.name = 'ProtocolError';
  }
}

/** A remote method is unknown, failed, timed out, or its connection dropped. */
export class RpcError extends Error {
  /**
   * @param {string} method
   * @param {string} message
   */
  constructor(method, message) {
    super(message);
    this.name = 'RpcError';
    /** The method name that was called. */
    this.method = method;
  }
}

/**
 * One unit crossing the wire.
 *
 * `header` and `correlationId` are overloaded by `type`: the header is an RPC method name, a topic,
 * or a stream id, and the correlation id matches a reply to its request, indexes a stream chunk, or
 * counts the messages inside a batch.
 */
export class Message {
  /**
   * @param {number} type
   * @param {string} [header]
   * @param {Buffer|Uint8Array} [payload]
   * @param {number} [correlationId]
   * @param {number} [flags]
   */
  constructor(type, header = '', payload = Buffer.alloc(0), correlationId = 0, flags = MessageFlags.None) {
    this.type = type;
    this.header = header;
    this.payload = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
    this.correlationId = correlationId;
    this.flags = flags;
  }

  /** True when the peer reported a failure instead of a result. */
  get isError() {
    return (this.flags & MessageFlags.Error) !== 0;
  }

  /** Decode the payload as UTF-8. */
  text() {
    return this.payload.toString('utf8');
  }

  /** @param {number} type @param {string} header @param {string} payload */
  static text(type, header, payload) {
    return new Message(type, header, Buffer.from(payload, 'utf8'));
  }

  toString() {
    return `<${messageTypeName(this.type)} ${JSON.stringify(this.header)} ${this.payload.length}B #${this.correlationId}>`;
  }
}

/**
 * Metadata carried by a StreamStart message, so a receiver knows what is coming before the first
 * chunk lands.
 */
export class StreamDescriptor {
  /** Marks a stream whose size is not known up front. */
  static UNKNOWN_LENGTH = -1;

  /**
   * @param {string} [name]
   * @param {number} [totalLength]
   * @param {string} [contentType]
   */
  constructor(name = '', totalLength = -1, contentType = 'application/octet-stream') {
    this.name = name;
    this.totalLength = totalLength;
    this.contentType = contentType;
  }

  /** True when the sender declared a size. */
  get hasLength() {
    return this.totalLength >= 0;
  }

  /** Serialise to the StreamStart payload layout. */
  encode() {
    const name = Buffer.from(this.name, 'utf8');
    const contentType = Buffer.from(this.contentType, 'utf8');
    const buffer = Buffer.allocUnsafe(12 + name.length + contentType.length);

    buffer.writeBigInt64LE(BigInt(this.totalLength), 0);
    buffer.writeUInt16LE(name.length, 8);
    name.copy(buffer, 10);
    buffer.writeUInt16LE(contentType.length, 10 + name.length);
    contentType.copy(buffer, 12 + name.length);
    return buffer;
  }

  /**
   * Parse a StreamStart payload. A short payload yields an unnamed descriptor.
   * @param {Buffer} payload
   * @returns {StreamDescriptor}
   */
  static decode(payload) {
    if (!payload || payload.length < 10) return new StreamDescriptor();

    const totalLength = Number(payload.readBigInt64LE(0));
    const nameLength = payload.readUInt16LE(8);
    if (payload.length < 10 + nameLength + 2) {
      return new StreamDescriptor('', totalLength, '');
    }

    const name = payload.toString('utf8', 10, 10 + nameLength);
    const contentTypeLength = payload.readUInt16LE(10 + nameLength);
    const start = 12 + nameLength;
    const contentType =
      payload.length >= start + contentTypeLength
        ? payload.toString('utf8', start, start + contentTypeLength)
        : '';

    return new StreamDescriptor(name, totalLength, contentType);
  }
}

/**
 * Serialise one message, length prefix included.
 * @param {Message} message
 * @returns {Buffer}
 */
export function encodeFrame(message) {
  const header = message.header ? Buffer.from(message.header, 'utf8') : Buffer.alloc(0);
  if (header.length > MAX_HEADER_LENGTH) {
    throw new ProtocolError(`Header is ${header.length} bytes; the limit is ${MAX_HEADER_LENGTH}.`);
  }

  const payload = message.payload ?? Buffer.alloc(0);
  const frameLength = FIXED_HEADER_SIZE + header.length + payload.length;
  if (frameLength > 0x7fffffff) {
    throw new ProtocolError('Frame exceeds int32 addressing.');
  }

  const frame = Buffer.allocUnsafe(PREFIX_SIZE + header.length + payload.length);
  frame.writeInt32LE(frameLength, 0);
  frame.writeUInt8(message.type, 4);
  frame.writeUInt8(message.flags ?? 0, 5);
  frame.writeUInt16LE(header.length, 6);
  frame.writeBigInt64LE(BigInt(message.correlationId ?? 0), 8);
  header.copy(frame, PREFIX_SIZE);
  payload.copy(frame, PREFIX_SIZE + header.length);
  return frame;
}

/**
 * Parse one frame starting at `offset`.
 *
 * Returns `{ message, consumed }`, or `null` when the buffer does not hold a whole frame yet. The
 * returned payload is a copy, so it may outlive the read buffer.
 *
 * @param {Buffer} buffer
 * @param {number} [offset]
 * @param {number} [maxFrameLength]
 * @returns {{ message: Message, consumed: number } | null}
 */
export function decodeFrame(buffer, offset = 0, maxFrameLength = DEFAULT_MAX_FRAME_LENGTH) {
  const available = buffer.length - offset;
  if (available < LENGTH_PREFIX_SIZE) return null;

  const frameLength = buffer.readInt32LE(offset);
  if (frameLength < FIXED_HEADER_SIZE) {
    throw new ProtocolError(
      `Frame length ${frameLength} is below the ${FIXED_HEADER_SIZE}-byte minimum; the stream is out of sync.`,
    );
  }
  if (frameLength > maxFrameLength) {
    throw new ProtocolError(`Frame length ${frameLength} exceeds the ${maxFrameLength}-byte limit.`);
  }
  if (available < LENGTH_PREFIX_SIZE + frameLength) return null;

  const type = buffer.readUInt8(offset + 4);
  const flags = buffer.readUInt8(offset + 5);
  const headerLength = buffer.readUInt16LE(offset + 6);
  // Correlation ids are counters and chunk indices, always well inside Number's safe range.
  const correlationId = Number(buffer.readBigInt64LE(offset + 8));

  if (FIXED_HEADER_SIZE + headerLength > frameLength) {
    throw new ProtocolError(`Header length ${headerLength} does not fit in a ${frameLength}-byte frame.`);
  }

  const headerStart = offset + PREFIX_SIZE;
  const header = headerLength ? buffer.toString('utf8', headerStart, headerStart + headerLength) : '';

  const payloadStart = headerStart + headerLength;
  const payloadLength = frameLength - FIXED_HEADER_SIZE - headerLength;
  const payload = payloadLength
    ? Buffer.from(buffer.subarray(payloadStart, payloadStart + payloadLength))
    : Buffer.alloc(0);

  return {
    message: new Message(type, header, payload, correlationId, flags),
    consumed: LENGTH_PREFIX_SIZE + frameLength,
  };
}

/**
 * Match a topic against an MQTT-style filter, where `+` matches exactly one segment and `#` matches
 * the remainder.
 *
 * This mirrors `TopicFilter.Matches` in the .NET library, including one place it is stricter than
 * the MQTT specification: `sensor/#` does **not** match the bare parent topic `sensor`, because `#`
 * must have at least one segment to swallow. The broker decides delivery, so agreeing with it
 * matters more than agreeing with the spec.
 *
 * @param {string} filter
 * @param {string} topic
 * @returns {boolean}
 */
export function topicMatches(filter, topic) {
  const filterParts = filter.split('/');
  const topicParts = topic.split('/');

  for (let index = 0; index < filterParts.length; index++) {
    const part = filterParts[index];
    if (part === '#') return index < topicParts.length;
    if (index >= topicParts.length) return false;
    if (part !== '+' && part !== topicParts[index]) return false;
  }

  return filterParts.length === topicParts.length;
}
