import { canonicalize, canonicalContent, bytesEqual, sha256, type Hasher, type CanonicalContent } from './canonical.js';
import { encodeFrame, decodeFrame, FrameType } from './frame.js';
import type { Volume } from './volume.js';
import {
  IdempotencyConflictError,
  KeyNotVisibleError,
  UnknownNamespaceError,
} from './errors.js';

export interface EventRecord {
  sequence: number;
  stream: string;
  payload: unknown;
}

export interface AppendRequest {
  stream: string;
  payload: unknown;
  /** idempotency key; without one the append is unconditional */
  key?: string;
  /** idempotency namespace; defaults to "default" */
  namespace?: string;
  signal?: AbortSignal;
}

export interface EventAppendResult extends EventRecord {
  /** true when this call did not append a new event but returned the earlier one */
  replay: boolean;
  key?: string;
  namespace?: string;
}

export interface EventLogOptions {
  volume: Volume;
  /** per-namespace retention window in ms; "default" applies to keyed appends without a namespace */
  namespaces?: Record<string, number>;
  /** retention ms for the default namespace when it is not listed (24h) */
  defaultRetentionMs?: number;
  /** content hash function (sha256 by default; inject a lossy one to test collisions) */
  hasher?: Hasher;
  now?: () => number;
  /** commits automatically advance the all-replica visibility watermark */
  autoVisible?: boolean;
}

export interface KeyStatus {
  sequence: number;
  digest: string;
  expiresAt: number;
}

export interface ConflictStatus {
  existingDigest: string;
  incomingDigest: string;
  existingSequence: number | null;
  at: number;
  expiresAt: number;
}

interface IndexEntry extends KeyStatus {
  /** canonical bytes of the winning content; final arbiter when digests collide */
  canonical: Uint8Array;
}

/* Wire payloads (JSON-encoded inside frames) */
interface EventWire {
  t: 'e';
  s: string;
  p: unknown;
  ns?: string;
  k?: string;
  d?: string;
  exp?: number;
}
interface ConflictWire {
  t: 'c';
  ns: string;
  k: string;
  ed: string;
  id: string;
  es: number | null;
  at: number;
  exp: number;
}
interface WatermarkWire {
  t: 'w';
  w: number;
}
type Wire = EventWire | ConflictWire | WatermarkWire;

const DEFAULT_NAMESPACE = 'default';
const DEFAULT_RETENTION_MS = 24 * 60 * 60 * 1000;

class AbortedOperationError extends Error {
  constructor() {
    super('The operation was aborted');
    this.name = 'AbortError';
  }
}

function abortError(signal: AbortSignal): Error {
  return (signal.reason as Error) ?? new AbortedOperationError();
}

function anySignal(signals: (AbortSignal | undefined)[]): AbortSignal | undefined {
  const list = signals.filter((s): s is AbortSignal => Boolean(s));
  if (!list.length) return undefined;
  if (list.length === 1) return list[0];
  const ac = new AbortController();
  for (const s of list) {
    if (s.aborted) ac.abort(s.reason);
    else s.addEventListener('abort', () => ac.abort(s.reason), { once: true });
  }
  return ac.signal;
}

/**
 * Commit mutex. Every append/recovery-mutating operation runs inside it, so
 * two concurrent requests with the same key serialize completely: exactly
 * one content wins, the other sees the finished result and replays or
 * conflicts. A request waiting on the mutex with an aborted signal never
 * runs — its turn in the chain is released without touching the log.
 */
class Mutex {
  #chain: Promise<void> = Promise.resolve();

  async acquire(signal?: AbortSignal): Promise<() => void> {
    const prev = this.#chain;
    let release!: () => void;
    this.#chain = new Promise<void>((resolve) => {
      release = resolve;
    });
    if (signal?.aborted) {
      release();
      throw abortError(signal);
    }
    if (signal) {
      const aborted = new Promise<never>((_, reject) => {
        signal.addEventListener('abort', () => reject(abortError(signal)), { once: true });
      });
      await Promise.race([prev, aborted]);
      if (signal.aborted) {
        release();
        throw abortError(signal);
      }
    } else {
      await prev;
    }
    return release;
  }
}

export class EventLog {
  readonly #volume: Volume;
  readonly #retention: Map<string, number>;
  readonly #hasher: Hasher;
  readonly #now: () => number;
  readonly #autoVisible: boolean;
  readonly #mutex = new Mutex();

  #events: EventRecord[] = [];
  /** namespace -> key -> winning index entry */
  readonly #index = new Map<string, Map<string, IndexEntry>>();
  /** namespace -> key -> latest conflict diagnostics */
  readonly #conflicts = new Map<string, Map<string, ConflictStatus>>();
  #visibilityWatermark = 0;

  private constructor(opts: EventLogOptions) {
    this.#volume = opts.volume;
    this.#hasher = opts.hasher ?? sha256;
    this.#now = opts.now ?? (() => Date.now());
    this.#autoVisible = opts.autoVisible ?? false;
    this.#retention = new Map(Object.entries(opts.namespaces ?? {}));
    if (!this.#retention.has(DEFAULT_NAMESPACE)) {
      this.#retention.set(DEFAULT_NAMESPACE, opts.defaultRetentionMs ?? DEFAULT_RETENTION_MS);
    }
  }

  static async open(opts: EventLogOptions): Promise<EventLog> {
    const log = new EventLog(opts);
    await log.#recover();
    return log;
  }

  // ---------------------------------------------------------------- public

  /** append one event; idempotent when `key` is supplied */
  async append(
    stream: string,
    payload: unknown,
    options: { key?: string; namespace?: string; signal?: AbortSignal } = {},
  ): Promise<EventAppendResult> {
    const [result] = await this.appendMany([
      { stream, payload, key: options.key, namespace: options.namespace, signal: options.signal },
    ]);
    return result;
  }

  /**
   * Append a batch atomically: all events commit (and are indexed) in one
   * framed group with one flush, or nothing does. Requests carrying the same
   * key collapse: the first occurrence appends, later occurrences with equal
   * content return its sequence (replay = true), different content makes the
   * whole batch fail with IdempotencyConflictError after diagnostics persist.
   */
  async appendMany(requests: AppendRequest[]): Promise<EventAppendResult[]> {
    if (!requests.length) return [];
    const signal = anySignal(requests.map((r) => r.signal));
    const release = await this.#mutex.acquire(signal);
    try {
      if (signal?.aborted) throw abortError(signal);

      const now = this.#now();
      const plans = requests.map(() => ({ kind: 'new' as 'new' | 'replay', eventIndex: -1 }));
      /** new keyed/unkeyed events to persist */
      const newEvents: { wire: EventWire; payload: unknown; stream: string }[] = [];
      /** canonical bytes for entries in newEvents, indexed the same way */
      const canonicalByEvent: CanonicalContent[] = [];
      /** (namespace, key) -> newEvents index, for same-batch duplicates */
      const batchKeys = new Map<string, number>();

      for (let i = 0; i < requests.length; i++) {
        const req = requests[i];
        if (req.key === undefined) {
          newEvents.push({ wire: { t: 'e', s: req.stream, p: req.payload }, payload: req.payload, stream: req.stream });
          plans[i] = { kind: 'new', eventIndex: newEvents.length - 1 };
          continue;
        }

        const ns = req.namespace ?? DEFAULT_NAMESPACE;
        const retention = this.#retention.get(ns);
        if (retention === undefined) throw new UnknownNamespaceError(ns);

        const content = await canonicalContent(req.payload, this.#hasher);

        const existing = this.#index.get(ns)?.get(req.key);
        if (existing) {
          if (existing.expiresAt <= now) {
            // Retention elapsed. Recycling is still forbidden while lagging
            // replicas may not have observed the original event.
            if (existing.sequence > this.#visibilityWatermark) {
              throw new KeyNotVisibleError(ns, req.key, existing.sequence, this.#visibilityWatermark);
            }
            this.#index.get(ns)!.delete(req.key);
            // fall through: expired-and-visible key may be reused
          } else if (existing.digest === content.digest && bytesEqual(existing.canonical, content.bytes)) {
            plans[i] = { kind: 'replay', eventIndex: -1 };
            continue;
          } else {
            await this.#persistConflict(ns, req.key, existing, content, retention, now);
            throw new IdempotencyConflictError({
              namespace: ns,
              key: req.key,
              existingDigest: existing.digest,
              incomingDigest: content.digest,
              existingSequence: existing.sequence,
            });
          }
        }

        const batchKey = `${ns}/${req.key}`;
        const earlier = batchKeys.get(batchKey);
        if (earlier !== undefined) {
          const priorWire = newEvents[earlier].wire;
          const priorDigest = priorWire.d!;
          const priorCanonical = canonicalByEvent[earlier];
          if (priorDigest === content.digest && bytesEqual(priorCanonical.bytes, content.bytes)) {
            plans[i] = { kind: 'replay', eventIndex: earlier };
            continue;
          }
          // Both contents are in this same uncommitted batch, so neither has
          // a durable sequence yet; diagnostics retain both digests.
          await this.#persistConflictRaw(ns, req.key, priorDigest, content.digest, null, retention, now);
          throw new IdempotencyConflictError({
            namespace: ns,
            key: req.key,
            existingDigest: priorDigest,
            incomingDigest: content.digest,
            existingSequence: null,
          });
        }

        const wire: EventWire = {
          t: 'e',
          s: req.stream,
          p: req.payload,
          ns,
          k: req.key,
          d: content.digest,
          exp: now + retention,
        };
        newEvents.push({ wire, payload: req.payload, stream: req.stream });
        const eventIndex = newEvents.length - 1;
        batchKeys.set(batchKey, eventIndex);
        canonicalByEvent[eventIndex] = content;
        plans[i] = { kind: 'new', eventIndex };
      }

      if (signal?.aborted) throw abortError(signal);

      // Single atomic group: events + idempotency data + visibility update,
      // one flush for the whole commit boundary.
      const base = this.#events.length;
      const frames: Uint8Array[] = [encodeFrame(FrameType.Batch, new Uint8Array(0))];
      newEvents.forEach((e) => frames.push(encodeFrame(FrameType.Event, encoder.encode(JSON.stringify(e.wire)))));
      let newVisibility = this.#visibilityWatermark;
      if (this.#autoVisible && newEvents.length) newVisibility = base + newEvents.length;
      if (newVisibility > this.#visibilityWatermark) {
        const w: WatermarkWire = { t: 'w', w: newVisibility };
        frames.push(encodeFrame(FrameType.Watermark, encoder.encode(JSON.stringify(w))));
      }
      frames.push(encodeFrame(FrameType.Commit, new Uint8Array(0)));
      await this.#writeAndFlush(frames);

      // Memory state mutates only after the flush succeeded, so an in-memory
      // index can never reference an event the log does not contain.
      const sequences: number[] = [];
      newEvents.forEach((e, idx) => {
        const sequence = base + idx + 1;
        sequences.push(sequence);
        this.#events.push({ sequence, stream: e.stream, payload: e.payload });
        if (e.wire.k !== undefined && e.wire.ns !== undefined) {
          this.#indexFor(e.wire.ns).set(e.wire.k!, {
            sequence,
            digest: e.wire.d!,
            canonical: canonicalByEvent[idx].bytes,
            expiresAt: e.wire.exp!,
          });
        }
      });
      this.#visibilityWatermark = newVisibility;
      this.#collectGarbage(this.#now());

      const results: EventAppendResult[] = plans.map((plan, i) => {
        const req = requests[i];
        if (plan.kind === 'replay') {
          if (plan.eventIndex === -1) {
            const entry = this.#index.get(req.namespace ?? DEFAULT_NAMESPACE)!.get(req.key!)!;
            const record = this.#events[entry.sequence - 1];
            return { ...record, replay: true, key: req.key, namespace: req.namespace ?? DEFAULT_NAMESPACE };
          }
          const sequence = sequences[plan.eventIndex];
          const e = newEvents[plan.eventIndex];
          return {
            sequence,
            stream: e.stream,
            payload: e.payload,
            replay: true,
            key: req.key,
            namespace: req.namespace ?? DEFAULT_NAMESPACE,
          };
        }
        const sequence = sequences[plan.eventIndex];
        return {
          sequence,
          stream: req.stream,
          payload: req.payload,
          replay: false,
          ...(req.key !== undefined ? { key: req.key, namespace: req.namespace ?? DEFAULT_NAMESPACE } : {}),
        };
      });

      // A timeout that lands while the flush is in flight: the commit still
      // completes (it must — a retry expects the original sequence), but the
      // already-gone-up caller receives its abort error.
      if (signal?.aborted) throw abortError(signal);
      return results;
    } finally {
      release();
    }
  }

  /**
   * Advance the all-replica visibility watermark. Idempotent index entries
   * whose event is at or below this watermark AND whose retention window has
   * elapsed become eligible for recycling; the watermark itself is persisted.
   */
  async advanceVisibilityWatermark(sequence: number, signal?: AbortSignal): Promise<void> {
    const release = await this.#mutex.acquire(signal);
    try {
      if (signal?.aborted) throw abortError(signal);
      const next = Math.min(sequence, this.#events.length);
      if (next <= this.#visibilityWatermark) return;
      const wire: WatermarkWire = { t: 'w', w: next };
      await this.#writeAndFlush([
        encodeFrame(FrameType.Batch, new Uint8Array(0)),
        encodeFrame(FrameType.Watermark, encoder.encode(JSON.stringify(wire))),
        encodeFrame(FrameType.Commit, new Uint8Array(0)),
      ]);
      this.#visibilityWatermark = next;
      this.#collectGarbage(this.#now());
    } finally {
      release();
    }
  }

  read(from = 1): EventRecord[] {
    return this.#events.filter((event) => event.sequence >= from);
  }

  watermark(): number {
    return this.#events.length;
  }

  visibilityWatermark(): number {
    return this.#visibilityWatermark;
  }

  lookupKey(namespace: string | undefined, key: string): KeyStatus | undefined {
    const entry = this.#index.get(namespace ?? DEFAULT_NAMESPACE)?.get(key);
    if (!entry) return undefined;
    return { sequence: entry.sequence, digest: entry.digest, expiresAt: entry.expiresAt };
  }

  /** retained two-content digest diagnostics for a conflicting key */
  getConflict(namespace: string | undefined, key: string): ConflictStatus | undefined {
    return this.#conflicts.get(namespace ?? DEFAULT_NAMESPACE)?.get(key);
  }

  // ------------------------------------------------------------- internals

  #indexFor(namespace: string): Map<string, IndexEntry> {
    let map = this.#index.get(namespace);
    if (!map) {
      map = new Map();
      this.#index.set(namespace, map);
    }
    return map;
  }

  async #persistConflict(
    namespace: string,
    key: string,
    existing: IndexEntry,
    incoming: CanonicalContent,
    retention: number,
    now: number,
  ): Promise<void> {
    await this.#persistConflictRaw(namespace, key, existing.digest, incoming.digest, existing.sequence, retention, now);
  }

  async #persistConflictRaw(
    namespace: string,
    key: string,
    existingDigest: string,
    incomingDigest: string,
    existingSequence: number | null,
    retention: number,
    now: number,
  ): Promise<void> {
    const wire: ConflictWire = {
      t: 'c',
      ns: namespace,
      k: key,
      ed: existingDigest,
      id: incomingDigest,
      es: existingSequence,
      at: now,
      exp: now + retention,
    };
    await this.#writeAndFlush([
      encodeFrame(FrameType.Batch, new Uint8Array(0)),
      encodeFrame(FrameType.Conflict, encoder.encode(JSON.stringify(wire))),
      encodeFrame(FrameType.Commit, new Uint8Array(0)),
    ]);
    let map = this.#conflicts.get(namespace);
    if (!map) {
      map = new Map();
      this.#conflicts.set(namespace, map);
    }
    map.set(key, {
      existingDigest,
      incomingDigest,
      existingSequence,
      at: now,
      expiresAt: now + retention,
    });
  }

  async #writeAndFlush(frames: Uint8Array[]): Promise<void> {
    const total = frames.reduce((n, f) => n + f.length, 0);
    const all = new Uint8Array(total);
    let offset = 0;
    for (const f of frames) {
      all.set(f, offset);
      offset += f.length;
    }
    await this.#volume.append(all);
    await this.#volume.flush();
  }

  /**
   * Delete index entries only when BOTH conditions hold:
   *   1. retention window elapsed (expiresAt <= now)
   *   2. the winning event is visible on every replica (seq <= watermark)
   * Conflict diagnostics have no visibility dependency and expire on time.
   */
  #collectGarbage(now: number): void {
    for (const map of this.#index.values()) {
      for (const [key, entry] of map) {
        if (entry.expiresAt <= now && entry.sequence <= this.#visibilityWatermark) map.delete(key);
      }
    }
    for (const map of this.#conflicts.values()) {
      for (const [key, conflict] of map) {
        if (conflict.expiresAt <= now) map.delete(key);
      }
    }
  }

  async #recover(): Promise<void> {
    const bytes = await this.#volume.read();
    const pending: Wire[] = [];
    let inGroup = false;
    let offset = 0;
    let commitEnd = 0;

    scan: while (offset < bytes.length) {
      const frame = decodeFrame(bytes, offset);
      if (!frame) break; // torn or corrupt tail: everything from here is discarded
      switch (frame.type) {
        case FrameType.Batch:
          inGroup = true;
          pending.length = 0;
          break;
        case FrameType.Commit:
          if (inGroup) {
            this.#applyReplayed(pending);
            pending.length = 0;
            inGroup = false;
            commitEnd = frame.end;
          }
          break;
        case FrameType.Event:
        case FrameType.Conflict:
        case FrameType.Watermark:
          if (!inGroup) break scan;
          pending.push(JSON.parse(decoder.decode(frame.payload)) as Wire);
          break;
        default:
          break scan;
      }
      offset = frame.end;
    }

    // Remove any bytes that never reached a commit boundary (crash mid-group
    // or torn tail), so the on-disk prefix equals the rebuilt state.
    if (bytes.length > commitEnd) await this.#volume.truncate(commitEnd);
    this.#collectGarbage(this.#now());
  }

  #applyReplayed(ops: Wire[]): void {
    for (const op of ops) {
      if (op.t === 'e') {
        const sequence = this.#events.length + 1;
        this.#events.push({ sequence, stream: op.s, payload: op.p });
        if (op.ns !== undefined && op.k !== undefined && op.d !== undefined && op.exp !== undefined) {
          // Re-canonicalize from the stored payload; byte equality after
          // recovery uses the same deterministic encoding as live traffic.
          const canonical = encoder.encode(canonicalize(op.p));
          this.#indexFor(op.ns).set(op.k, {
            sequence,
            digest: op.d,
            canonical,
            expiresAt: op.exp,
          });
        }
      } else if (op.t === 'c') {
        let map = this.#conflicts.get(op.ns);
        if (!map) {
          map = new Map();
          this.#conflicts.set(op.ns, map);
        }
        map.set(op.k, {
          existingDigest: op.ed,
          incomingDigest: op.id,
          existingSequence: op.es,
          at: op.at,
          expiresAt: op.exp,
        });
      } else if (op.t === 'w') {
        if (op.w > this.#visibilityWatermark) this.#visibilityWatermark = op.w;
      }
    }
  }
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();
