/**
 * Codec tests that need no server.
 *
 * BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
 */

import test from 'node:test';
import assert from 'node:assert/strict';

import {
  FIXED_HEADER_SIZE,
  Message,
  MessageFlags,
  MessageType,
  PREFIX_SIZE,
  ProtocolError,
  StreamDescriptor,
  decodeFrame,
  encodeFrame,
  messageTypeName,
  topicMatches,
} from '../src/index.js';

test('round trips every field', () => {
  const original = new Message(
    MessageType.RpcRequest,
    'sensor/tank-3/temperature',
    Buffer.from('28.4'),
    987654321,
    MessageFlags.NoReply,
  );

  const frame = encodeFrame(original);
  const parsed = decodeFrame(frame);

  assert.equal(parsed.consumed, frame.length);
  assert.equal(parsed.message.type, original.type);
  assert.equal(parsed.message.flags, original.flags);
  assert.equal(parsed.message.header, original.header);
  assert.equal(parsed.message.correlationId, original.correlationId);
  assert.deepEqual(parsed.message.payload, original.payload);
});

test('frame layout matches the specification', () => {
  const frame = encodeFrame(new Message(MessageType.Publish, 'ab', Buffer.from('xyz'), 7));

  assert.equal(frame.readInt32LE(0), FIXED_HEADER_SIZE + 2 + 3);
  assert.equal(frame.readUInt8(4), MessageType.Publish);
  assert.equal(frame.readUInt8(5), 0);
  assert.equal(frame.readUInt16LE(6), 2);
  assert.equal(Number(frame.readBigInt64LE(8)), 7);
  assert.equal(frame.toString('utf8', PREFIX_SIZE, PREFIX_SIZE + 2), 'ab');
  assert.equal(frame.toString('utf8', PREFIX_SIZE + 2), 'xyz');
});

test('handles an empty header and payload', () => {
  const { message } = decodeFrame(encodeFrame(new Message(MessageType.Ping)));
  assert.equal(message.type, MessageType.Ping);
  assert.equal(message.header, '');
  assert.equal(message.payload.length, 0);
});

test('returns null until the whole frame arrives', () => {
  const frame = encodeFrame(new Message(MessageType.Publish, 'topic', Buffer.from('body')));

  for (let prefix = 0; prefix < frame.length; prefix++) {
    assert.equal(decodeFrame(frame.subarray(0, prefix)), null, `prefix ${prefix}`);
  }
  assert.notEqual(decodeFrame(frame), null);
});

test('parses back-to-back frames', () => {
  const stream = Buffer.concat(
    Array.from({ length: 5 }, (_, i) =>
      encodeFrame(new Message(MessageType.Publish, `topic/${i}`, Buffer.from(String(i)))),
    ),
  );

  const seen = [];
  let offset = 0;
  for (;;) {
    const parsed = decodeFrame(stream, offset);
    if (!parsed) break;
    offset += parsed.consumed;
    seen.push(parsed.message.header);
  }

  assert.deepEqual(seen, ['topic/0', 'topic/1', 'topic/2', 'topic/3', 'topic/4']);
  assert.equal(offset, stream.length);
});

test('handles non-ASCII headers and payloads', () => {
  const header = 'suhu/tangki/derajat-°C';
  const body = '28,4 °C — αβγ — 日本語 — 🕳';

  const { message } = decodeFrame(
    encodeFrame(new Message(MessageType.Publish, header, Buffer.from(body, 'utf8'))),
  );

  assert.equal(message.header, header);
  assert.equal(message.text(), body);
});

test('rejects a frame longer than the limit', () => {
  const frame = encodeFrame(new Message(MessageType.Publish, 't', Buffer.alloc(4096)));
  assert.throws(() => decodeFrame(frame, 0, 128), ProtocolError);
});

test('rejects an impossible length prefix', () => {
  assert.throws(() => decodeFrame(Buffer.from([2, 0, 0, 0, 1, 2])), /out of sync/);
});

test('rejects an oversized header', () => {
  assert.throws(() => encodeFrame(new Message(MessageType.Publish, 'x'.repeat(70000))), /Header is/);
});

test('preserves an unknown message type', () => {
  // A newer peer may know message types this client does not; that is not a framing error.
  const frame = encodeFrame(new Message(MessageType.Publish, 't'));
  frame.writeUInt8(0x7e, 4);

  const { message } = decodeFrame(frame);
  assert.equal(message.type, 0x7e);
  assert.equal(messageTypeName(0x7e), 'Unknown(0x7E)');
});

test('a decoded payload survives the read buffer being reused', () => {
  const frame = encodeFrame(new Message(MessageType.Publish, 't', Buffer.from('keep me')));
  const { message } = decodeFrame(frame);

  frame.fill(0); // Simulate the socket buffer being overwritten.
  assert.equal(message.text(), 'keep me');
});

test('StreamDescriptor round trips', () => {
  const original = new StreamDescriptor('kalibrasi-2026.csv', 1_048_576, 'text/csv');
  const parsed = StreamDescriptor.decode(original.encode());

  assert.equal(parsed.name, original.name);
  assert.equal(parsed.totalLength, original.totalLength);
  assert.equal(parsed.contentType, original.contentType);
  assert.equal(parsed.hasLength, true);
});

test('StreamDescriptor handles an unknown length', () => {
  const original = new StreamDescriptor('live.log', -1, 'text/plain');
  const parsed = StreamDescriptor.decode(original.encode());

  assert.equal(parsed.totalLength, -1);
  assert.equal(parsed.hasLength, false);
  assert.equal(StreamDescriptor.decode(Buffer.alloc(0)).hasLength, false);
});

test('topic matching mirrors the broker', () => {
  const cases = [
    ['sensor/tank-3/temp', 'sensor/tank-3/temp', true],
    ['sensor/+/temp', 'sensor/tank-3/temp', true],
    ['sensor/+/temp', 'sensor/tank-3/humidity', false],
    ['sensor/+/temp', 'sensor/a/b/temp', false],
    ['sensor/#', 'sensor/tank-3/temp', true],
    ['sensor/#', 'sensor', false],
    ['#', 'anything/at/all', true],
    ['sensor/tank-3/temp', 'sensor/tank-3', false],
    ['sensor/tank-3', 'sensor/tank-3/temp', false],
    ['+/+/temp', 'sensor/tank-3/temp', true],
  ];

  for (const [filter, topic, expected] of cases) {
    assert.equal(topicMatches(filter, topic), expected, `${filter} vs ${topic}`);
  }
});
