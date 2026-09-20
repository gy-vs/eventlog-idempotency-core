import { crc32 } from './crc32.js';

/**
 * Binary framing for the durable log.
 *
 * Frame layout (all integers big-endian):
 *   magic   4 bytes  FRAM
 *   type    1 byte
 *   length  4 bytes  payload length
 *   crc     4 bytes  crc32 of the payload
 *   payload length bytes
 *
 * An appender must write a BATCH frame before each atomic group and an
 * equivalent COMMIT frame after it. On replay every frame after the last
 * COMMIT is discarded: a crash anywhere between the last fsync and the final
 * one of the group leaves no partial group visible, which guarantees the
 * group (events + idempotency information) shares a single commit boundary.
 */
export const FRAME_MAGIC = 0x4652414d; // 'FRAM'

export enum FrameType {
  Event = 1,
  Conflict = 2,
  Watermark = 3,
  Batch = 4,
  Commit = 5,
}

export function encodeFrame(type: FrameType, payload: Uint8Array): Uint8Array {
  const frame = new Uint8Array(13 + payload.length);
  const view = new DataView(frame.buffer);
  view.setUint32(0, FRAME_MAGIC);
  view.setUint8(4, type);
  view.setUint32(5, payload.length);
  view.setUint32(9, crc32(payload));
  frame.set(payload, 13);
  return frame;
}

export interface DecodedFrame {
  type: FrameType;
  payload: Uint8Array;
  /** offset of the first byte after this frame */
  end: number;
}

/**
 * Reads a single frame at `offset`. Returns null when the frame is absent,
 * truncated, or corrupt (torn tail after a crash). `shortRead` distinguishes
 * "nothing more to read" from "a frame header started but did not survive".
 */
export function decodeFrame(buf: Uint8Array, offset: number): DecodedFrame | null {
  if (offset + 13 > buf.length) return null;
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  if (view.getUint32(offset) !== FRAME_MAGIC) return null;
  const type = view.getUint8(offset + 4) as FrameType;
  const length = view.getUint32(offset + 5);
  const storedCrc = view.getUint32(offset + 9);
  const end = offset + 13 + length;
  if (end > buf.length) return null;
  const payload = buf.subarray(offset + 13, end);
  if (crc32(payload) !== storedCrc) return null;
  return { type, payload, end };
}
