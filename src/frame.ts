import { createHash } from 'node:crypto';

/**
 * On-disk frame:
 *   magic "ELG1" | uint32 LE payload length | payload JSON bytes | 32-byte SHA-256
 *
 * The checksum covers magic + length + payload. A crash during writeback can
 * leave a truncated or otherwise torn final frame; replay stops at the first
 * invalid frame and the tail is truncated, so partial commits never become
 * visible.
 */
const MAGIC = 0x3147_4c45; // "ELG1" little-endian
const HEADER_LEN = 8;
const CHECKSUM_LEN = 32;

export interface KeyRegistration {
  ns: string;
  key: string;
  digest: string;
  canonical: number[];
  seq: number;
  expiresAt: number;
}

export interface ConflictRecord extends KeyRegistration {
  otherDigest: string;
  otherCanonical: number[];
  reason: 'different-content' | 'digest-collision';
  detectedAt: number;
  /** Sequence of the winning record, when the winner was already committed. */
  otherSeq?: number;
}

/** Atomic commit: all events and registrations appear together or not at all. */
export interface CommitFrame {
  type: 'commit';
  events: { stream: string; payload: unknown }[];
  keys: KeyRegistration[];
  /** Visibility watermark snapshot embedded opportunistically. */
  visibility?: number;
}

export interface ConflictFrame {
  type: 'conflict';
  records: ConflictRecord[];
  visibility?: number;
}

export interface WatermarkFrame {
  type: 'watermark';
  /** replica id -> highest contiguous sequence it has observed */
  replicas: Record<string, number>;
  visibility: number;
}

export type Frame = CommitFrame | ConflictFrame | WatermarkFrame;

function checksum(data: Uint8Array): Uint8Array {
  return createHash('sha256').update(data).digest();
}

export function encodeFrame(frame: Frame): Uint8Array {
  const payload = new TextEncoder().encode(JSON.stringify(frame));
  const total = HEADER_LEN + payload.length + CHECKSUM_LEN;
  const buf = new Uint8Array(total);
  const dv = new DataView(buf.buffer);
  dv.setUint32(0, MAGIC, true);
  dv.setUint32(4, payload.length, true);
  buf.set(payload, HEADER_LEN);
  buf.set(checksum(buf.subarray(0, HEADER_LEN + payload.length)), HEADER_LEN + payload.length);
  return buf;
}

export class FrameCorruptionError extends Error {
  constructor(
    message: string,
    readonly offset: number,
  ) {
    super(`${message} at offset ${offset}`);
    this.name = 'FrameCorruptionError';
  }
}

/**
 * Parse every intact frame from `bytes`. Returns the frames and the byte
 * offset of the first torn/invalid tail (or bytes.length if none).
 */
export function readFrames(bytes: Uint8Array): { frames: Frame[]; validOffset: number } {
  const frames: Frame[] = [];
  let offset = 0;
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

  while (offset < bytes.length) {
    if (bytes.length - offset < HEADER_LEN) break; // torn header
    const magic = dv.getUint32(offset, true);
    if (magic !== MAGIC) throw new FrameCorruptionError('bad magic', offset);
    const len = dv.getUint32(offset + 4, true);
    const end = offset + HEADER_LEN + len + CHECKSUM_LEN;
    if (end > bytes.length) break; // truncated payload/checksum
    const body = bytes.subarray(offset, offset + HEADER_LEN + len);
    const actual = bytes.subarray(offset + HEADER_LEN + len, end);
    const expected = checksum(body);
    let ok = expected.length === actual.length;
    for (let i = 0; ok && i < expected.length; i++) ok = expected[i] === actual[i];
    if (!ok) throw new FrameCorruptionError('checksum mismatch', offset);
    let frame: Frame;
    try {
      frame = JSON.parse(
        new TextDecoder().decode(bytes.subarray(offset + HEADER_LEN, offset + HEADER_LEN + len)),
      );
    } catch (err) {
      throw new FrameCorruptionError(`invalid payload: ${(err as Error).message}`, offset);
    }
    if (typeof frame !== 'object' || frame === null || typeof (frame as Frame).type !== 'string') {
      throw new FrameCorruptionError('malformed frame', offset);
    }
    frames.push(frame);
    offset = end;
  }

  return { frames, validOffset: offset };
}
