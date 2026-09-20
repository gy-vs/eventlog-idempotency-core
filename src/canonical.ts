import { createHash } from 'node:crypto';

/**
 * Thrown when a payload cannot be deterministically encoded: JSON-only values
 * are accepted (null, boolean, finite number, string, plain object, array).
 */
export class CanonicalizeError extends TypeError {}

function isPlainObject(value: object): boolean {
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/**
 * Deterministic JSON encoding (RFC 8785 style): object keys sorted by UTF-16
 * code unit, no insignificant whitespace, arrays in order. Field order in the
 * caller's objects can never change the resulting bytes.
 */
export function canonicalize(value: unknown): Uint8Array {
  const out: string[] = [];
  const seen = new Set<object>();

  const emit = (v: unknown): void => {
    if (v === null) {
      out.push('null');
      return;
    }
    const t = typeof v;
    if (t === 'string') {
      out.push(JSON.stringify(v));
      return;
    }
    if (t === 'number') {
      if (!Number.isFinite(v as number)) throw new CanonicalizeError('non-finite number');
      out.push(JSON.stringify(v));
      return;
    }
    if (t === 'boolean') {
      out.push(v ? 'true' : 'false');
      return;
    }
    if (t === 'bigint' || t === 'undefined' || t === 'function' || t === 'symbol') {
      throw new CanonicalizeError(`${t} values are not supported`);
    }
    const o = v as object;
    if (seen.has(o)) throw new CanonicalizeError('cyclic value');
    seen.add(o);
    if (Array.isArray(o)) {
      out.push('[');
      for (let i = 0; i < o.length; i++) {
        if (i > 0) out.push(',');
        emit((o as unknown[])[i]);
      }
      out.push(']');
    } else if (isPlainObject(o)) {
      const keys = Object.keys(o as Record<string, unknown>).sort();
      out.push('{');
      for (let i = 0; i < keys.length; i++) {
        if (i > 0) out.push(',');
        out.push(JSON.stringify(keys[i]!), ':');
        emit((o as Record<string, unknown>)[keys[i]!]);
      }
      out.push('}');
    } else {
      throw new CanonicalizeError('only plain JSON objects are supported');
    }
    seen.delete(o);
  };

  emit(value);
  return new TextEncoder().encode(out.join(''));
}

export type DigestFn = (canonical: Uint8Array) => string;

/** Default content digest: full SHA-256, hex encoded. */
export const sha256Digest: DigestFn = (canonical) =>
  createHash('sha256').update(canonical).digest('hex');
