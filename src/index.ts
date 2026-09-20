import { type DigestFn, canonicalize, sha256Digest } from './canonical.js';
import { IdempotencyConflictError } from './errors.js';
import {
  type CommitFrame,
  type ConflictRecord,
  type Frame,
  type KeyRegistration,
  type WatermarkFrame,
  encodeFrame,
  readFrames,
} from './frame.js';
import type { LogHandle, LogStorage } from './storage.js';

export { IdempotencyConflictError } from './errors.js';
export type { ConflictDiagnostics } from './errors.js';
export { canonicalize, sha256Digest, CanonicalizeError } from './canonical.js';
export type { DigestFn } from './canonical.js';
export { FileStorage, MemoryStorage, CrashDisk } from './storage.js';
export type { LogHandle, LogStorage, CrashHooks } from './storage.js';
export type { ConflictRecord } from './frame.js';

export interface EventRecord {
  sequence: number;
  stream: string;
  payload: unknown;
}

export interface IdempotencyKey {
  namespace: string;
  key: string;
}

export interface AppendOptions {
  namespace?: string;
  key?: string;
  /** How long this key may deduplicate, from the commit time (ms). */
  retentionMs?: number;
}

export interface BatchItem {
  stream: string;
  payload: unknown;
  key?: IdempotencyKey;
  retentionMs?: number;
}

export type AppendOutcome =
  | { status: 'created'; event: EventRecord }
  | { status: 'replayed'; event: EventRecord }
  | { status: 'conflict'; error: IdempotencyConflictError };

export interface EventLogOptions {
  /** Default key retention window (ms). Defaults to 24h. */
  retentionMs?: number;
  /** Injected clock (tests). */
  now?: () => number;
  /** Injected digest (tests: hash collision simulation). */
  digest?: DigestFn;
}

interface IndexEntry {
  ns: string;
  key: string;
  seq: number;
  digest: string;
  canonical: Uint8Array;
  expiresAt: number;
}

type WaiterResult =
  | { kind: 'committed'; seq: number }
  | { kind: 'conflict'; error: IdempotencyConflictError }
  | { kind: 'aborted'; error: unknown };

interface Waiter {
  digest: string;
  canonical: Uint8Array;
  resolve: (result: WaiterResult) => void;
}

interface Claim {
  ns: string;
  key: string;
  digest: string;
  canonical: Uint8Array;
  expiresAt: number;
  waiters: Waiter[];
}

type Classified =
  | { kind: 'plain'; stream: string; payload: unknown }
  | { kind: 'replay'; seq: number }
  | { kind: 'conflict'; error: IdempotencyConflictError; record: ConflictRecord }
  | { kind: 'wait'; promise: Promise<WaiterResult> }
  | { kind: 'claim'; stream: string; payload: unknown; claim: Claim };

const DEFAULT_NAMESPACE = 'default';
const DAY_MS = 24 * 60 * 60 * 1000;

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

const toArray = (bytes: Uint8Array): number[] => Array.from(bytes);

export class EventLog {
  readonly #handle: LogHandle;
  readonly #events: EventRecord[];
  readonly #index = new Map<string, IndexEntry>();
  readonly #inflight = new Map<string, Claim>();
  readonly #conflicts: ConflictRecord[];
  readonly #replicas: Map<string, number>;
  readonly #retentionMs: number;
  readonly #now: () => number;
  readonly #digest: DigestFn;
  /** Serializes every physical commit: write+fsync never interleave. */
  #chain: Promise<unknown> = Promise.resolve();

  private constructor(
    handle: LogHandle,
    events: EventRecord[],
    index: Map<string, IndexEntry>,
    conflicts: ConflictRecord[],
    replicas: Map<string, number>,
    options: EventLogOptions,
  ) {
    this.#handle = handle;
    this.#events = events;
    this.#conflicts = conflicts;
    this.#replicas = replicas;
    this.#retentionMs = options.retentionMs ?? DAY_MS;
    this.#now = options.now ?? (() => Date.now());
    this.#digest = options.digest ?? sha256Digest;
    for (const [fk, entry] of index) this.#index.set(fk, entry);
    this.#gc();
  }

  // ---------- recovery: the index is rebuilt purely from the log ----------

  static async open(storage: LogStorage, options: EventLogOptions = {}): Promise<EventLog> {
    const { bytes, handle } = await storage.open();
    const { frames, validOffset } = readFrames(bytes);
    if (validOffset < bytes.length) {
      // Torn tail from a crash mid-writeback: drop it so a partial commit can
      // never become visible.
      await handle.truncate(validOffset);
    }

    const events: EventRecord[] = [];
    const index = new Map<string, IndexEntry>();
    const conflicts: ConflictRecord[] = [];
    const replicas = new Map<string, number>();

    for (const frame of frames as Frame[]) {
      if (frame.type === 'commit') {
        const base = events.length;
        for (const ev of frame.events) {
          events.push({ sequence: events.length + 1, stream: ev.stream, payload: ev.payload });
        }
        for (const reg of frame.keys) {
          if (reg.seq <= base || reg.seq > events.length) {
            throw new Error(
              `corrupt index rebuild: key ${reg.ns}/${reg.key} points at seq ${reg.seq}`,
            );
          }
          index.set(`${reg.ns} ${reg.key}`, {
            ns: reg.ns,
            key: reg.key,
            seq: reg.seq,
            digest: reg.digest,
            canonical: Uint8Array.from(reg.canonical),
            expiresAt: reg.expiresAt,
          });
        }
      } else if (frame.type === 'conflict') {
        for (const rec of frame.records) conflicts.push(rec);
      } else if (frame.type === 'watermark') {
        for (const [id, seq] of Object.entries(frame.replicas)) {
          replicas.set(id, Math.max(replicas.get(id) ?? 0, seq));
        }
      }
    }

    return new EventLog(handle, events, index, conflicts, replicas, options);
  }

  // ---------- reads ----------

  read(from = 1): EventRecord[] {
    return this.#events.filter((event) => event.sequence >= from);
  }

  /** Local durable high-water mark. */
  watermark(): number {
    return this.#events.length;
  }

  /**
   * All-replica visibility watermark: highest sequence every known replica has
   * observed. Keys are never recycled past this point.
   */
  visibilityWatermark(): number {
    if (this.#replicas.size === 0) return 0;
    let min = Infinity;
    for (const seq of this.#replicas.values()) min = Math.min(min, seq);
    return Math.min(min, this.#events.length);
  }

  /** Persisted diagnostics: both content digests of every observed conflict. */
  conflicts(namespace?: string, key?: string): ConflictRecord[] {
    return this.#conflicts.filter(
      (rec) =>
        (namespace === undefined || rec.ns === namespace) &&
        (key === undefined || rec.key === key),
    );
  }

  // ---------- replica watermarks ----------

  async reportReplicaWatermark(replica: string, seq: number): Promise<void> {
    await this.#enqueue(async () => {
      const v = Math.max(0, Math.min(Math.trunc(seq), this.#events.length));
      if (v <= (this.#replicas.get(replica) ?? 0)) return;
      this.#replicas.set(replica, v);
      const frame: WatermarkFrame = {
        type: 'watermark',
        replicas: Object.fromEntries(this.#replicas),
        visibility: this.visibilityWatermark(),
      };
      await this.#writeAndSync(frame);
    });
  }

  // ---------- appends ----------

  async append(stream: string, payload: unknown, options: AppendOptions = {}): Promise<EventRecord> {
    const ns = options.namespace ?? DEFAULT_NAMESPACE;
    const item: BatchItem = { stream, payload };
    if (options.key !== undefined) item.key = { namespace: ns, key: options.key };
    if (options.retentionMs !== undefined) item.retentionMs = options.retentionMs;
    const [outcome] = await this.appendBatch([item]);
    if (outcome.status === 'conflict') throw outcome.error;
    return outcome.event;
  }

  async appendBatch(items: BatchItem[]): Promise<AppendOutcome[]> {
    // Phase 1 — synchronous classification. No await runs between reading the
    // index/inflight maps and inserting a claim, so concurrent same-key callers
    // are totally ordered: exactly one content wins. This whole prologue runs
    // before the first await (async functions execute synchronously up to it),
    // preserving that ordering across concurrent callers.
    const now = this.#now();
    const insertedClaims: Claim[] = [];
    let classified: Classified[];
    try {
      classified = items.map((item) => {
        const canonical = canonicalize(item.payload);
        const digest = this.#digest(canonical);

        if (!item.key) return { kind: 'plain', stream: item.stream, payload: item.payload };

        const { namespace: ns, key } = item.key;
        const fk = `${ns} ${key}`;
        const existing = this.#index.get(fk);

        if (existing) {
          const expired = existing.expiresAt <= now && existing.seq <= this.visibilityWatermark();
          if (!expired) return this.#classifyAgainst(existing, digest, canonical, now);
          // Retention elapsed AND every replica has seen it: the slot is reusable.
          this.#index.delete(fk);
        }

        const prior = this.#inflight.get(fk);
        if (prior) {
          const promise = new Promise<WaiterResult>((resolve) => {
            prior.waiters.push({ digest, canonical, resolve });
          });
          return { kind: 'wait', promise };
        }

        const claim: Claim = {
          ns,
          key,
          digest,
          canonical,
          expiresAt: now + (item.retentionMs ?? this.#retentionMs),
          waiters: [],
        };
        this.#inflight.set(fk, claim);
        insertedClaims.push(claim);
        return { kind: 'claim', stream: item.stream, payload: item.payload, claim };
      });
    } catch (error) {
      // A bad payload rejects the whole batch before any write; roll back every
      // claim this prologue reserved so the keys stay free.
      for (const claim of insertedClaims) {
        this.#inflight.delete(`${claim.ns} ${claim.key}`);
      }
      throw error;
    }

    // Phase 2 — physical commit, serialized after every earlier batch.
    const commit = this.#enqueue(() => this.#commitPhase(classified));
    return this.#assemble(classified, commit);
  }

  #classifyAgainst(
    existing: IndexEntry,
    digest: string,
    canonical: Uint8Array,
    now: number,
  ): Classified {
    if (digest === existing.digest && sameBytes(canonical, existing.canonical)) {
      return { kind: 'replay', seq: existing.seq };
    }
    const reason: ConflictRecord['reason'] =
      digest === existing.digest ? 'digest-collision' : 'different-content';
    const record: ConflictRecord = {
      ns: existing.ns,
      key: existing.key,
      digest: existing.digest,
      canonical: toArray(existing.canonical),
      seq: existing.seq,
      expiresAt: existing.expiresAt,
      otherDigest: digest,
      otherCanonical: toArray(canonical),
      reason,
      detectedAt: now,
    };
    return { kind: 'conflict', error: this.#conflictError(record), record };
  }

  /**
   * One serialized physical step. Returns the seq assigned to each plain/claim
   * item (undefined for other kinds).
   */
  async #commitPhase(classified: Classified[]): Promise<(number | undefined)[]> {
    const claims = new Set<Claim>();
    const earlyConflicts: ConflictRecord[] = [];
    for (const c of classified) {
      if (c.kind === 'claim') claims.add(c.claim);
      if (c.kind === 'conflict') earlyConflicts.push(c.record);
    }

    const seqs: (number | undefined)[] = new Array(classified.length).fill(undefined);

    try {
      this.#gc();

      const newItems = classified.flatMap((c, i) =>
        c.kind === 'plain' || c.kind === 'claim' ? [{ c, i }] : [],
      );

      if (newItems.length > 0) {
        // Compute frame and sequences from tentative values, but publish NOTHING
        // observable until fsync has crossed the durability boundary.
        const base = this.#events.length;
        const keys: KeyRegistration[] = [];
        const frameEvents: CommitFrame['events'] = [];
        const assigned: { c: (typeof newItems)[number]['c']; seq: number }[] = [];

        newItems.forEach(({ c, i }, n) => {
          const seq = base + n + 1;
          seqs[i] = seq;
          frameEvents.push({ stream: c.stream, payload: c.payload });
          assigned.push({ c, seq });
          if (c.kind === 'claim') {
            keys.push({
              ns: c.claim.ns,
              key: c.claim.key,
              digest: c.claim.digest,
              canonical: toArray(c.claim.canonical),
              seq,
              expiresAt: c.claim.expiresAt,
            });
          }
        });

        // ONE frame, ONE fsync: log events and idempotency registrations share
        // a commit boundary. A crash can only expose both or neither.
        const frame: CommitFrame = {
          type: 'commit',
          events: frameEvents,
          keys,
          visibility: this.visibilityWatermark(),
        };
        await this.#writeAndSync(frame);

        // Durable: publish events and index entries together.
        for (const { c, seq } of assigned) {
          this.#events.push({ sequence: seq, stream: c.stream, payload: c.payload });
        }
        for (const { c, seq } of assigned) {
          if (c.kind === 'claim') {
            this.#index.set(`${c.claim.ns} ${c.claim.key}`, {
              ns: c.claim.ns,
              key: c.claim.key,
              seq,
              digest: c.claim.digest,
              canonical: c.claim.canonical,
              expiresAt: c.claim.expiresAt,
            });
          }
        }
      }

      // Concurrent same-key waiters: equal content shares the winning result;
      // different content gets a conflict. Full byte comparison defends even
      // against digest collisions.
      const waiterConflicts: ConflictRecord[] = [];
      const now = this.#now();
      const settlements: { waiter: Waiter; result: WaiterResult }[] = [];

      for (const claim of claims) {
        const entry = this.#index.get(`${claim.ns} ${claim.key}`);
        const seq = entry?.seq;

        for (const waiter of claim.waiters) {
          let result: WaiterResult;
          if (waiter.digest === claim.digest && sameBytes(waiter.canonical, claim.canonical)) {
            result = { kind: 'committed', seq: seq! };
          } else {
            const reason: ConflictRecord['reason'] =
              waiter.digest === claim.digest ? 'digest-collision' : 'different-content';
            waiterConflicts.push({
              ns: claim.ns,
              key: claim.key,
              digest: claim.digest,
              canonical: toArray(claim.canonical),
              seq: seq!,
              expiresAt: claim.expiresAt,
              otherDigest: waiter.digest,
              otherCanonical: toArray(waiter.canonical),
              reason,
              detectedAt: now,
            });
            const record = waiterConflicts[waiterConflicts.length - 1]!;
            result = { kind: 'conflict', error: this.#conflictError(record) };
          }
          settlements.push({ waiter, result });
        }

        claim.waiters.length = 0;
        this.#inflight.delete(`${claim.ns} ${claim.key}`);
      }

      if (waiterConflicts.length > 0) {
        for (const record of waiterConflicts) this.#conflicts.push(record);
        // Diagnostics must be durable before the conflicting waiters are woken.
        await this.#writeAndSync({
          type: 'conflict',
          records: waiterConflicts,
          visibility: this.visibilityWatermark(),
        });
      }

      // Only now release concurrent callers.
      for (const { waiter, result } of settlements) waiter.resolve(result);

      // Conflicts against already-committed keys were known in phase 1; persist
      // them before this batch returns as well.
      if (earlyConflicts.length > 0) {
        for (const record of earlyConflicts) this.#conflicts.push(record);
        await this.#writeAndSync({
          type: 'conflict',
          records: earlyConflicts,
          visibility: this.visibilityWatermark(),
        });
      }

      return seqs;
    } catch (error) {
      // Nothing crossed the commit boundary: no events, no index entries, no
      // sequence numbers are observable. Abort waiters so callers retry.
      for (const claim of claims) {
        for (const waiter of claim.waiters) waiter.resolve({ kind: 'aborted', error });
        claim.waiters.length = 0;
        this.#inflight.delete(`${claim.ns} ${claim.key}`);
      }
      throw error;
    }
  }

  #conflictError(record: ConflictRecord): IdempotencyConflictError {
    return new IdempotencyConflictError({
      namespace: record.ns,
      key: record.key,
      storedDigest: record.digest,
      incomingDigest: record.otherDigest,
      storedSequence: record.seq,
      detectedAt: record.detectedAt,
    });
  }

  async #assemble(
    classified: Classified[],
    commit: Promise<(number | undefined)[]>,
  ): Promise<AppendOutcome[]> {
    const settled = new Array<AppendOutcome | undefined>(classified.length);
    const pending: Promise<void>[] = [];

    classified.forEach((c, i) => {
      if (c.kind === 'replay') {
        settled[i] = { status: 'replayed', event: this.#events[c.seq - 1]! };
      } else if (c.kind === 'conflict') {
        settled[i] = { status: 'conflict', error: c.error };
      } else if (c.kind === 'wait') {
        pending.push(
          c.promise.then((result) => {
            if (result.kind === 'aborted') throw result.error;
            if (result.kind === 'conflict') {
              settled[i] = { status: 'conflict', error: result.error };
            } else {
              settled[i] = { status: 'replayed', event: this.#events[result.seq - 1]! };
            }
          }),
        );
      }
    });

    const seqs = await commit;
    await Promise.all(pending);

    return classified.map((c, i): AppendOutcome => {
      const existing = settled[i];
      if (existing) return existing;
      if (c.kind === 'plain' || c.kind === 'claim') {
        const seq = seqs[i]!;
        return { status: 'created', event: this.#events[seq - 1]! };
      }
      throw new Error('unreachable outcome');
    });
  }

  // ---------- retention / visibility ----------

  #gc(): void {
    const now = this.#now();
    const visible = this.visibilityWatermark();
    for (const [fk, entry] of this.#index) {
      if (entry.expiresAt <= now && entry.seq <= visible) this.#index.delete(fk);
    }
  }

  // ---------- plumbing ----------

  #enqueue<T>(task: () => Promise<T>): Promise<T> {
    const run = this.#chain.then(task, task);
    this.#chain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  async #writeAndSync(frame: Frame): Promise<void> {
    await this.#handle.append(encodeFrame(frame));
    await this.#handle.sync();
  }

  async close(): Promise<void> {
    await this.#chain;
    await this.#handle.close();
  }
}
