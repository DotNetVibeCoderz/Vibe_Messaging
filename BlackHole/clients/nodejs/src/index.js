/**
 * BlackHole Messaging - Node.js client.
 *
 * Gravicode Studios, led by Kang Fadhil.
 *
 * A client for the BlackHole binary messaging protocol: RPC, Pub/Sub, Streaming and Batching over
 * TCP. Speaks the same wire format as the .NET library, verified against it by the interop suite.
 *
 * ```js
 * import { connect } from '@gravicode/blackhole-messaging';
 *
 * const client = await connect({ host: '127.0.0.1', port: 5000 });
 * console.log(await client.callText('upper', 'halo blackhole'));
 * await client.close();
 * ```
 *
 * @module
 */

export { BlackHoleClient, connect, connectIpc, resolveIpcPath } from './client.js';
export {
  DEFAULT_MAX_FRAME_LENGTH,
  FIXED_HEADER_SIZE,
  LENGTH_PREFIX_SIZE,
  MAX_HEADER_LENGTH,
  Message,
  MessageFlags,
  MessageType,
  PREFIX_SIZE,
  ProtocolError,
  RpcError,
  StreamDescriptor,
  decodeFrame,
  encodeFrame,
  messageTypeName,
  topicMatches,
} from './protocol.js';
