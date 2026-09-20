import { createHash } from 'node:crypto';

/**
 * Deterministic JSON encoding: object keys are sorted recursively so that
 * payload equality is independent of field insertion order. The output is
 * never parsed back; it exists only for hashing and byte comparison, so we
 * can use tokens that JSON itself cannot produce:
 *  - strings are always emitted quoted, bare tokens never are
 *  - undefined is the bare token `undefined`
 *  - objects -> {...}, arrays -> [...], nothing else starts with those
 * Non-finite numbers and bigints are rejected: they have no stable,
 * language-independent encoding.
 */
export function canonicalize(value: unknown): string {
  const seen = new Set<object>();

  const emit = (v: unknown): string => {
    if (v === null) return 'null';
    if (typeof v === 'undefined') return 'undefined';
    if (typeof v === 'boolean') return v ? 'true' : 'false';
    if (typeof v === 'number') {
      if (!Number.isFinite(v)) throw new TypeError('non-finite numbers cannot be canonicalized');
      return JSON.stringify(v);
    }
    if (typeof v === 'string') return JSON.stringify(v);
    if (typeof v === 'bigint') throw new TypeError('bigints cannot be canonicalized');
    if (typeof v === 'object') {
      if (seen.has(v)) throw new TypeError('cyclic payload cannot be canonicalized');
      seen.add(v);
      try {
        if (Array.isArray(v)) {
          let out = '[';
          for (let i = 0; i < v.length; i++) {
            if (i) out += ',';
            out += emit(v[i]);
          }
          return out + ']';
        }
        const keys = Object.keys(v as Record<string, unknown>).sort();
        let out = '{';
        for (let i = 0; i < keys.length; i++) {
          if (i) out += ',';
          out += `${JSON.stringify(keys[i])}:${emit((v as Record<string, unknown>)[keys[i]])}`;
        }
        return out + '}';
      } finally {
        seen.delete(v);
      }
    }
    // functions / symbols have no meaningful content identity
    throw new TypeError(`value of type ${typeof v} cannot be canonicalized`);
  };

  return emit(value);
}

export type Hasher = (bytes: Uint8Array) => string | Promise<string>;

export const sha256: Hasher = (bytes) => createHash('sha256').update(bytes).digest('hex');

export interface CanonicalContent {
  digest: string;
  bytes: Uint8Array;
}

export async function canonicalContent(value: unknown, hasher: Hasher = sha256): Promise<CanonicalContent> {
  const bytes = new TextEncoder().encode(canonicalize(value));
  return { bytes, digest: await hasher(bytes) };
}

export function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}
