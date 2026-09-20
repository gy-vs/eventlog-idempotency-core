import { describe, expect, it, vi } from 'vitest';
import { join } from 'node:path';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import {
  EventLog,
  MemoryVolume,
  FileVolume,
  IdempotencyConflictError,
  KeyNotVisibleError,
  UnknownNamespaceError,
  type Volume,
} from '../src/index.js';
import { encodeFrame, decodeFrame, FrameType } from '../src/frame.js';
import { canonicalize, sha256 } from '../src/canonical.js';

const NS = { default: 60_000, payments: 10_000 };

interface Clock {
  now(): number;
  set(t: number): void;
  advance(ms: number): void;
}

function clock(start = 1_000_000): Clock {
  let t = start;
  return {
    now: () => t,
    set: (v: number) => {
      t = v;
    },
    advance: (ms: number) => {
      t += ms;
    },
  };
}

async function openLog(opts: Partial<Parameters<typeof EventLog.open>[0]> = {}) {
  return EventLog.open({
    volume: new MemoryVolume(),
    namespaces: NS,
    now: opts.now ?? clock().now,
    ...opts,
  });
}

/** Volume that can stall the first flush (modelling a slow fsync). */
class GateVolume implements Volume {
  #inner: Volume;
  #waiters: Array<() => void> = [];
  blockFirstFlush = false;

  constructor(inner: Volume) {
    this.#inner = inner;
  }

  get inner(): Volume {
    return this.#inner;
  }

  pendingFlushes(): number {
    return this.#waiters.length;
  }

  releaseFlush(): void {
    const waiters = this.#waiters;
    this.#waiters = [];
    waiters.forEach((w) => w());
  }

  append(data: Uint8Array): void | Promise<void> {
    return this.#inner.append(data);
  }

  async flush(): Promise<void> {
    if (this.blockFirstFlush) {
      this.blockFirstFlush = false;
      await new Promise<void>((resolve) => this.#waiters.push(resolve));
    }
    await this.#inner.flush();
  }

  read(): Uint8Array | Promise<Uint8Array> {
    return this.#inner.read();
  }

  truncate(size: number): void | Promise<void> {
    return this.#inner.truncate(size);
  }

  crash(): void | Promise<void> {
    return this.#inner.crash();
  }

  close(): void | Promise<void> {
    return this.#inner.close();
  }
}

function assertAborted(p: Promise<unknown>): Promise<void> {
  return p.then(
    () => {
      throw new Error('expected abort');
    },
    (err: unknown) => {
      expect(String((err as Error).name)).toBe('AbortError');
    },
  );
}

describe('basic ordering (backward compatible)', () => {
  it('appends in order', async () => {
    const x = await openLog();
    await x.append('a', 1);
    expect(x.read()[0].sequence).toBe(1);
    expect(x.watermark()).toBe(1);
  });

  it('read(from) filters by sequence', async () => {
    const x = await openLog();
    await x.append('a', 1);
    await x.append('a', 2);
    expect(x.read(2).map((e) => e.payload)).toEqual([2]);
  });
});

describe('idempotent replay', () => {
  it('same key + same content returns the original sequence and writes one event', async () => {
    const log = await openLog();
    const first = await log.append('orders', { id: 7, amount: 100 }, { key: 'k1' });
    const second = await log.append('orders', { id: 7, amount: 100 }, { key: 'k1' });
    expect(first.sequence).toBe(1);
    expect(first.replay).toBe(false);
    expect(second.sequence).toBe(1);
    expect(second.replay).toBe(true);
    expect(log.watermark()).toBe(1);
  });

  it('same key + different content conflicts and retains both content digests', async () => {
    const log = await openLog();
    await log.append('orders', { amount: 100 }, { key: 'k1' });
    const diagA = await sha256(new TextEncoder().encode(canonicalize({ amount: 100 })));
    const diagB = await sha256(new TextEncoder().encode(canonicalize({ amount: 200 })));
    await expect(log.append('orders', { amount: 200 }, { key: 'k1' })).rejects.toMatchObject({
      code: 'IDEMPOTENCY_CONFLICT',
      existingDigest: diagA,
      incomingDigest: diagB,
      existingSequence: 1,
    });
    expect(log.watermark()).toBe(1);
    const conflict = log.getConflict(undefined, 'k1')!;
    expect(conflict.existingDigest).toBe(diagA);
    expect(conflict.incomingDigest).toBe(diagB);
    expect(conflict.existingSequence).toBe(1);
  });

  it('keys are scoped by namespace', async () => {
    const log = await openLog();
    await log.append('s', { v: 1 }, { key: 'dup', namespace: 'payments' });
    const other = await log.append('s', { v: 1 }, { key: 'dup', namespace: 'default' });
    expect(other.sequence).toBe(2);
    await expect(
      log.append('s', { v: 2 }, { key: 'dup', namespace: 'payments' }),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);
  });

  it('unknown namespace is rejected', async () => {
    const log = await openLog();
    await expect(log.append('s', 1, { key: 'k', namespace: 'nope' })).rejects.toBeInstanceOf(
      UnknownNamespaceError,
    );
  });
});

describe('concurrent appends with the same key', () => {
  it('exactly one content wins; equal concurrent requests all get the same sequence', async () => {
    const log = await openLog();
    const payload = { x: 1 };
    const results = await Promise.all(
      Array.from({ length: 8 }, () => log.append('s', payload, { key: 'hot' })),
    );
    expect(results.map((r) => r.sequence)).toEqual(Array.from({ length: 8 }, () => 1));
    expect(results.filter((r) => !r.replay)).toHaveLength(1);
    expect(log.watermark()).toBe(1);
  });

  it('one winner appends; racing requests with different content get the conflict', async () => {
    const log = await openLog();
    const [winner, ...losers] = await Promise.allSettled([
      log.append('s', { v: 'a' }, { key: 'hot' }),
      ...Array.from({ length: 5 }, (_, i) => log.append('s', { v: `b${i}` }, { key: 'hot' })),
    ]);
    // Either the first settler wins; verify exactly one event and 5 conflicts.
    expect(winner.status).toBe('fulfilled');
    expect(log.watermark()).toBe(1);
    for (const r of losers) {
      expect(r.status).toBe('rejected');
      if (r.status === 'rejected') {
        expect(r.reason).toBeInstanceOf(IdempotencyConflictError);
        expect(r.reason.existingSequence).toBe(1);
      }
    }
  });

  it('a queued request whose timeout fires before acquiring the lock never runs', async () => {
    const volume = new MemoryVolume();
    const gate = new GateVolume(volume);
    gate.blockFirstFlush = true;
    const c = clock();
    const log = await EventLog.open({ volume: gate, namespaces: NS, now: c.now });

    const inFlight = log.append('s', 1, { key: 'k1' });
    await vi.waitFor(() => expect(gate.pendingFlushes()).toBe(1));

    const ac = new AbortController();
    const queued = log.append('s', 2, { key: 'k2', signal: ac.signal });
    ac.abort();
    await assertAborted(queued);

    gate.releaseFlush();
    await inFlight;
    expect(log.watermark()).toBe(1); // queued append never executed
  });
});

describe('first call timeout', () => {
  it('timeout during fsync aborts the client but the retry replays the same sequence', async () => {
    const gate = new GateVolume(new MemoryVolume());
    gate.blockFirstFlush = true;
    const log = await EventLog.open({ volume: gate, namespaces: NS });

    const ac = new AbortController();
    const call = log.append('orders', { amount: 100 }, { key: 'k1', signal: ac.signal });
    await vi.waitFor(() => expect(gate.pendingFlushes()).toBe(1));
    ac.abort();
    // The in-flight commit runs to completion despite the timeout; the caller
    // only learns about the abort afterwards.
    gate.releaseFlush();
    await assertAborted(call);

    const status = log.lookupKey(undefined, 'k1')!;
    expect(status.sequence).toBe(1);

    // Client retries the same key + same content: no second event.
    const retry = await log.append('orders', { amount: 100 }, { key: 'k1' });
    expect(retry.sequence).toBe(1);
    expect(retry.replay).toBe(true);
    expect(log.watermark()).toBe(1);
  });

  it('timeout after commit still lets a retry replay (commit never rolls back)', async () => {
    const gate = new GateVolume(new MemoryVolume());
    const log = await EventLog.open({ volume: gate, namespaces: NS });
    gate.blockFirstFlush = true;
    const ac = new AbortController();
    const call = log.append('s', { x: 1 }, { key: 'k1', signal: ac.signal });
    await vi.waitFor(() => expect(gate.pendingFlushes()).toBe(1));
    gate.releaseFlush();
    // The flush promise resolves before the abort is observed; abort may race
    // the in-memory state update. Either outcome must allow an exact replay.
    ac.abort();
    await expect(call.then(() => 'ok', () => 'aborted')).resolves.toMatch(/ok|aborted/);
    const retry = await log.append('s', { x: 1 }, { key: 'k1' });
    expect(retry.sequence).toBe(1);
    expect(log.watermark()).toBe(1);
  });
});

describe('crashes around fsync', () => {
  it('crash before fsync: nothing committed; rebuild starts empty and retry appends once', async () => {
    const mem = new MemoryVolume();
    const gate = new GateVolume(mem);
    gate.blockFirstFlush = true;
    const log = await EventLog.open({ volume: gate, namespaces: NS });

    const ac = new AbortController();
    const call = log.append('orders', { amount: 100 }, { key: 'k1', signal: ac.signal });
    await vi.waitFor(() => expect(gate.pendingFlushes()).toBe(1));
    await gate.crash(); // power loss: unflushed bytes discarded
    gate.releaseFlush();
    await expect(call).rejects.toThrow(); // abort or the crash itself

    const recovered = await EventLog.open({
      volume: new MemoryVolume(mem.snapshotBytes()),
      namespaces: NS,
    });
    expect(recovered.watermark()).toBe(0);
    expect(recovered.lookupKey(undefined, 'k1')).toBeUndefined();

    const retry = await recovered.append('orders', { amount: 100 }, { key: 'k1' });
    expect(retry.sequence).toBe(1);
    expect(retry.replay).toBe(false);
  });

  it('crash after fsync but before returning: rebuild replays the original sequence', async () => {
    const mem = new MemoryVolume();
    const gate = new GateVolume(mem);
    gate.blockFirstFlush = true;
    const log = await EventLog.open({ volume: gate, namespaces: NS });

    const ac = new AbortController();
    const call = log.append('orders', { amount: 100 }, { key: 'k1', signal: ac.signal });
    await vi.waitFor(() => expect(gate.pendingFlushes()).toBe(1));
    gate.releaseFlush();
    await vi.waitFor(() => expect(log.watermark()).toBe(1));
    await mem.crash(); // process dies immediately after durable commit
    ac.abort();
    await expect(call).resolves.toBeDefined();

    const recovered = await EventLog.open({
      volume: new MemoryVolume(mem.snapshotBytes()),
      namespaces: NS,
    });
    expect(recovered.watermark()).toBe(1);
    const retry = await recovered.append('orders', { amount: 100 }, { key: 'k1' });
    expect(retry.sequence).toBe(1);
    expect(retry.replay).toBe(true);
  });

  it('torn tail (partial frame survived) is truncated on rebuild', async () => {
    const mem = new MemoryVolume();
    const log = await EventLog.open({ volume: mem, namespaces: NS });
    await log.append('s', 1, { key: 'k1' });
    const good = mem.snapshotBytes();
    // append garbage + a truncated frame header directly to durable bytes
    const junk = new Uint8Array(good.length + 7);
    junk.set(good);
    junk.set([0x46, 0x52, 0x41, 0x4d, 1, 0, 0], good.length);
    const damaged = new MemoryVolume(junk);
    const recovered = await EventLog.open({ volume: damaged, namespaces: NS });
    expect(recovered.watermark()).toBe(1);
    expect((await damaged.read()).length).toBe(good.length);
    expect((await recovered.append('s', 1, { key: 'k1' })).replay).toBe(true);
  });

  it('crash inside a multi-event batch group discards the whole group', async () => {
    const mem = new MemoryVolume();
    const gate = new GateVolume(mem);
    gate.blockFirstFlush = true;
    const log = await EventLog.open({ volume: gate, namespaces: NS });

    const call = log.appendMany([
      { stream: 's', payload: 1, key: 'a' },
      { stream: 's', payload: 2 },
      { stream: 's', payload: 3, key: 'c' },
    ]);
    await vi.waitFor(() => expect(gate.pendingFlushes()).toBe(1));
    await gate.crash();
    gate.releaseFlush();
    await expect(call).rejects.toThrow(/crashed/);

    // Whole group vanished: no events and no index entries.
    const recovered = await EventLog.open({
      volume: new MemoryVolume(mem.snapshotBytes()),
      namespaces: NS,
    });
    expect(recovered.watermark()).toBe(0);
    expect(recovered.lookupKey(undefined, 'a')).toBeUndefined();
    expect(recovered.lookupKey(undefined, 'c')).toBeUndefined();
  });
});

describe('expiry, retention windows and visibility watermark', () => {
  it('entry is not recycled while the original event is below the visibility watermark', async () => {
    const c = clock();
    const log = await EventLog.open({ volume: new MemoryVolume(), namespaces: NS, now: c.now });
    await log.append('s', 1, { key: 'k1' });
    c.advance(120_000); // default retention (60s) elapsed, watermark still 0
    await expect(log.append('s', 2, { key: 'k1' })).rejects.toMatchObject({
      code: 'KEY_NOT_VISIBLE',
      sequence: 1,
      visibilityWatermark: 0,
    });
    expect(log.watermark()).toBe(1);
  });

  it('after watermark advance + expiry the key can be reused for a new event', async () => {
    const c = clock();
    const log = await EventLog.open({ volume: new MemoryVolume(), namespaces: NS, now: c.now });
    await log.append('s', { v: 1 }, { key: 'k1' });
    await log.advanceVisibilityWatermark(1);
    c.advance(120_000);
    const second = await log.append('s', { v: 2 }, { key: 'k1' });
    expect(second.sequence).toBe(2);
    expect(second.replay).toBe(false);
  });

  it('within the retention window the key keeps replaying even when visible', async () => {
    const c = clock();
    const log = await EventLog.open({ volume: new MemoryVolume(), namespaces: NS, now: c.now });
    await log.append('s', { v: 1 }, { key: 'k1' });
    await log.advanceVisibilityWatermark(1);
    c.advance(1_000);
    expect((await log.append('s', { v: 1 }, { key: 'k1' })).replay).toBe(true);
  });

  it('namespace retention windows differ', async () => {
    const c = clock();
    const log = await EventLog.open({ volume: new MemoryVolume(), namespaces: NS, now: c.now });
    await log.append('s', 1, { key: 'k', namespace: 'payments' }); // 10s
    await log.append('s', 1, { key: 'k' }); // default 60s
    await log.advanceVisibilityWatermark(2);
    c.advance(11_000);
    expect((await log.append('s', 2, { key: 'k', namespace: 'payments' })).replay).toBe(false);
    expect((await log.append('s', 1, { key: 'k' })).replay).toBe(true);
  });

  it('watermark never moves beyond the durable tail and is monotonic', async () => {
    const log = await openLog();
    await log.append('s', 1);
    await log.advanceVisibilityWatermark(99);
    expect(log.visibilityWatermark()).toBe(1);
    await log.advanceVisibilityWatermark(1);
    expect(log.visibilityWatermark()).toBe(1);
  });

  it('autoVisible commits the watermark in the same boundary', async () => {
    const mem = new MemoryVolume();
    const log = await EventLog.open({ volume: mem, namespaces: NS, autoVisible: true });
    await log.append('s', 1, { key: 'k1' });
    expect(log.visibilityWatermark()).toBe(1);
    const recovered = await EventLog.open({
      volume: new MemoryVolume(mem.snapshotBytes()),
      namespaces: NS,
      autoVisible: true,
    });
    expect(recovered.visibilityWatermark()).toBe(1);
  });
});

describe('hash collision simulation', () => {
  it('identical digest but different canonical bytes is a conflict, not a replay', async () => {
    // Deliberately lossy hash: every payload has the same digest.
    const broken = async () => 'deadbeef';
    const log = await EventLog.open({ volume: new MemoryVolume(), namespaces: NS, hasher: broken });
    await log.append('s', { amount: 100 }, { key: 'k1' });
    await expect(log.append('s', { amount: 200 }, { key: 'k1' })).rejects.toMatchObject({
      code: 'IDEMPOTENCY_CONFLICT',
      existingDigest: 'deadbeef',
      incomingDigest: 'deadbeef',
    });
    expect(log.watermark()).toBe(1);
  });

  it('byte-equal content still replays under the lossy hash', async () => {
    const broken = async () => 'deadbeef';
    const log = await EventLog.open({ volume: new MemoryVolume(), namespaces: NS, hasher: broken });
    await log.append('s', { amount: 100 }, { key: 'k1' });
    const replay = await log.append('s', { amount: 100 }, { key: 'k1' });
    expect(replay.sequence).toBe(1);
    expect(replay.replay).toBe(true);
  });

  it('collision safety survives index rebuild', async () => {
    const mem = new MemoryVolume();
    const broken = async () => 'deadbeef';
    const log = await EventLog.open({ volume: mem, namespaces: NS, hasher: broken });
    await log.append('s', { amount: 100 }, { key: 'k1' });
    const recovered = await EventLog.open({
      volume: new MemoryVolume(mem.snapshotBytes()),
      namespaces: NS,
      hasher: broken,
    });
    await expect(recovered.append('s', { amount: 200 }, { key: 'k1' })).rejects.toBeInstanceOf(
      IdempotencyConflictError,
    );
  });
});

describe('batch appends with partial duplication', () => {
  it('duplicate keyed entries collapse onto one event; the rest append', async () => {
    const log = await openLog();
    await log.append('s', { shared: 1 }, { key: 'k1' });
    const results = await log.appendMany([
      { stream: 's', payload: { shared: 1 }, key: 'k1' },
      { stream: 's', payload: 42 },
      { stream: 's', payload: { fresh: true }, key: 'k2' },
    ]);
    expect(results.map((r) => [r.sequence, r.replay])).toEqual([
      [1, true],
      [2, false],
      [3, false],
    ]);
    expect(log.watermark()).toBe(3);
  });

  it('same key twice in one batch with equal content: one event, two replays', async () => {
    const log = await openLog();
    const results = await log.appendMany([
      { stream: 's', payload: { v: 9 }, key: 'k1' },
      { stream: 's', payload: { v: 9 }, key: 'k1' },
    ]);
    expect(results[0].sequence).toBe(1);
    expect(results[0].replay).toBe(false);
    expect(results[1].sequence).toBe(1);
    expect(results[1].replay).toBe(true);
    expect(log.watermark()).toBe(1);
  });

  it('same key twice in one batch with different content: nothing is appended, conflict persists', async () => {
    const log = await openLog();
    await expect(
      log.appendMany([
        { stream: 's', payload: { v: 1 }, key: 'k1' },
        { stream: 's', payload: { v: 2 }, key: 'k1' },
      ]),
    ).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(log.watermark()).toBe(0);
    expect(log.getConflict(undefined, 'k1')).toMatchObject({
      existingSequence: null,
    });
  });

  it('batch is atomic across the commit boundary', async () => {
    const mem = new MemoryVolume();
    const gate = new GateVolume(mem);
    gate.blockFirstFlush = true;
    const log = await EventLog.open({ volume: gate, namespaces: NS });
    const call = log.appendMany([
      { stream: 's', payload: 1, key: 'a' },
      { stream: 's', payload: 2, key: 'b' },
    ]);
    await vi.waitFor(() => expect(gate.pendingFlushes()).toBe(1));
    await gate.crash();
    gate.releaseFlush();
    await expect(call).rejects.toThrow(/crashed/);
    const recovered = await EventLog.open({
      volume: new MemoryVolume(mem.snapshotBytes()),
      namespaces: NS,
    });
    expect(recovered.watermark()).toBe(0);
    expect(recovered.lookupKey(undefined, 'a')).toBeUndefined();
    expect(recovered.lookupKey(undefined, 'b')).toBeUndefined();
  });
});

describe('index rebuild from the log', () => {
  it('rebuilds entries, conflict diagnostics and the visibility watermark', async () => {
    const mem = new MemoryVolume();
    const c = clock();
    const first = await EventLog.open({ volume: mem, namespaces: NS, now: c.now });
    await first.append('orders', { amount: 100 }, { key: 'k1' });
    await first.append('orders', { amount: 5 }, { key: 'k2', namespace: 'payments' });
    await expect(first.append('orders', { amount: 999 }, { key: 'k1' })).rejects.toBeInstanceOf(
      IdempotencyConflictError,
    );
    await first.advanceVisibilityWatermark(2);
    const bytes = mem.snapshotBytes();

    const rebuilt = await EventLog.open({
      volume: new MemoryVolume(bytes),
      namespaces: NS,
      now: c.now,
    });
    expect(rebuilt.watermark()).toBe(2);
    expect(rebuilt.read()).toHaveLength(2);
    expect(rebuilt.lookupKey(undefined, 'k1')?.sequence).toBe(1);
    expect(rebuilt.lookupKey('payments', 'k2')?.sequence).toBe(2);
    const conflict = rebuilt.getConflict(undefined, 'k1')!;
    expect(conflict.existingDigest).not.toBe(conflict.incomingDigest);
    expect(conflict.existingSequence).toBe(1);
    expect(rebuilt.visibilityWatermark()).toBe(2);

    // Rebuilt index enforces idempotency exactly like the original.
    expect((await rebuilt.append('orders', { amount: 100 }, { key: 'k1' })).replay).toBe(true);
    await expect(rebuilt.append('orders', { amount: 101 }, { key: 'k1' })).rejects.toBeInstanceOf(
      IdempotencyConflictError,
    );
  });

  it('idempotency index and events share exactly one commit (no orphan index state)', async () => {
    // If the index were updated before flush, an abort here would leave a
    // dangling entry pointing at a non-existent event.
    const gate = new GateVolume(new MemoryVolume());
    gate.blockFirstFlush = true;
    const log = await EventLog.open({ volume: gate, namespaces: NS });
    const ac = new AbortController();
    const call = log.append('s', 1, { key: 'k1', signal: ac.signal });
    await vi.waitFor(() => expect(gate.pendingFlushes()).toBe(1));
    ac.abort();
    gate.releaseFlush();
    await assertAborted(call);
    const entry = log.lookupKey(undefined, 'k1')!;
    expect(log.read()).toHaveLength(1);
    expect(entry.sequence).toBe(log.read()[0].sequence);
  });

  it('expired entries are dropped on rebuild; live entries survive', async () => {
    const mem = new MemoryVolume();
    const c = clock();
    const first = await EventLog.open({ volume: mem, namespaces: NS, now: c.now });
    await first.append('s', 1, { key: 'live' });
    await first.append('s', 2, { key: 'old' });
    await first.advanceVisibilityWatermark(2);
    c.advance(120_000); // past default 60s retention
    const rebuilt = await EventLog.open({
      volume: new MemoryVolume(mem.snapshotBytes()),
      namespaces: NS,
      now: c.now,
    });
    expect(rebuilt.lookupKey(undefined, 'live')).toBeUndefined();
    expect(rebuilt.lookupKey(undefined, 'old')).toBeUndefined();
    expect(rebuilt.watermark()).toBe(2); // events themselves are never deleted
  });
});

describe('deterministic content encoding', () => {
  it('payloads differing only in object key field order are equal content', async () => {
    const log = await openLog();
    const a = { a: 1, nested: { z: 2, y: 3 }, arr: [9, 8] };
    const b = { nested: { y: 3, z: 2 }, arr: [9, 8], a: 1 };
    expect(canonicalize(a)).toBe(canonicalize(b));
    await log.append('s', a, { key: 'k1' });
    const replay = await log.append('s', b, { key: 'k1' });
    expect(replay.replay).toBe(true);
  });

  it('different array order or types are different content', async () => {
    const log = await openLog();
    await log.append('s', [1, 2, 3], { key: 'k1' });
    await expect(log.append('s', [3, 2, 1], { key: 'k1' })).rejects.toBeInstanceOf(
      IdempotencyConflictError,
    );
  });
});

describe('FileVolume durability', () => {
  it('persists through close/reopen and truncates torn tails', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'eventlog-'));
    try {
      const path = join(dir, 'log.bin');
      const v1 = await FileVolume.create(path);
      const log1 = await EventLog.open({ volume: v1, namespaces: NS });
      await log1.append('orders', { amount: 100 }, { key: 'k1' });
      await v1.close();

      const v2 = await FileVolume.create(path);
      const log2 = await EventLog.open({ volume: v2, namespaces: NS });
      expect(log2.watermark()).toBe(1);
      expect((await log2.append('orders', { amount: 100 }, { key: 'k1' })).replay).toBe(true);

      // physically tear the tail with a bad suffix and check recovery truncates
      const { appendFile, readFile } = await import('node:fs/promises');
      const good = await readFile(path);
      await appendFile(path, new Uint8Array([0x46, 0x52, 0x41, 0x4d, 1, 0, 0]));
      await v2.close();
      const v3 = await FileVolume.create(path);
      const log3 = await EventLog.open({ volume: v3, namespaces: NS });
      expect(log3.watermark()).toBe(1);
      const after = await readFile(path);
      expect(after.length).toBe(good.length);
      await v3.close();
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('frames are well-formed committed records on disk', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'eventlog-'));
    try {
      const path = join(dir, 'log.bin');
      const v = await FileVolume.create(path);
      const log = await EventLog.open({ volume: v, namespaces: NS });
      await log.append('s', { x: 1 }, { key: 'k1' });
      await v.close();
      const { readFile } = await import('node:fs/promises');
      const bytes = new Uint8Array(await readFile(path));
      // BATCH, EVENT, COMMIT
      const types: number[] = [];
      let off = 0;
      while (off < bytes.length) {
        const f = decodeFrame(bytes, off);
        if (!f) break;
        types.push(f.type);
        off = f.end;
      }
      expect(types).toEqual([FrameType.Batch, FrameType.Event, FrameType.Commit]);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
