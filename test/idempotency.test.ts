import { describe, expect, it } from 'vitest';
import type { EventLogOptions, LogHandle, LogStorage } from '../src/index.js';
import { EventLog, MemoryStorage, canonicalize } from '../src/index.js';

async function fresh(options: EventLogOptions = {}) {
  return EventLog.open(new MemoryStorage(), options);
}

describe('deterministic encoding', () => {
  it('ignores object field order', () => {
    const a = canonicalize({ a: 1, b: { x: 1, y: 2 }, c: [3, 4] });
    const b = canonicalize({ c: [3, 4], b: { y: 2, x: 1 }, a: 1 });
    expect(a).toEqual(b);
  });

  it('stabilizes nested key order but keeps array order', () => {
    expect(new TextDecoder().decode(canonicalize({ z: 1, a: 2 }))).toBe('{"a":2,"z":1}');
    expect(new TextDecoder().decode(canonicalize([1, 2, 3]))).toBe('[1,2,3]');
    expect(new TextDecoder().decode(canonicalize([{ b: 1, a: 2 }]))).toBe('[{"a":2,"b":1}]');
  });

  it('rejects non-JSON values', () => {
    expect(() => canonicalize(undefined)).toThrow();
    expect(() => canonicalize(1n)).toThrow();
    expect(() => canonicalize(() => 1)).toThrow();
    expect(() => canonicalize(NaN)).toThrow();
    class X {
      a = 1;
    }
    expect(() => canonicalize(new X())).toThrow();
    const cyclic: Record<string, unknown> = { a: 1 };
    cyclic.self = cyclic;
    expect(() => canonicalize(cyclic)).toThrow();
  });
});

describe('idempotent append', () => {
  it('replays the same sequence for the same key and content', async () => {
    const log = await fresh();
    const first = await log.append('s', { x: 1 }, { namespace: 'ns', key: 'k1' });
    const retry = await log.append('s', { x: 1 }, { namespace: 'ns', key: 'k1' });
    expect(first.sequence).toBe(1);
    expect(retry.sequence).toBe(1);
    expect(log.watermark()).toBe(1);
    expect(log.read()).toHaveLength(1);
  });

  it('is insensitive to field order when comparing content', async () => {
    const log = await fresh();
    await log.append('s', { a: 1, b: [2, 3] }, { key: 'k' });
    const replay = await log.append('s', { b: [2, 3], a: 1 }, { key: 'k' });
    expect(replay.sequence).toBe(1);
    expect(log.read()).toHaveLength(1);
  });

  it('flags conflicts and retains both content digests', async () => {
    const log = await fresh();
    await log.append('s', { v: 1 }, { key: 'k' });
    await expect(log.append('s', { v: 2 }, { key: 'k' })).rejects.toMatchObject({
      name: 'IdempotencyConflictError',
    });
    const stored = await log.append('s', { v: 1 }, { key: 'k' });
    expect(stored.sequence).toBe(1);
    expect(log.read()).toHaveLength(1);

    const records = log.conflicts();
    expect(records).toHaveLength(1);
    expect(records[0]!.reason).toBe('different-content');
    expect(records[0]!.digest).not.toBe(records[0]!.otherDigest);
    expect(records[0]!.seq).toBe(1);
  });

  it('isolates namespaces', async () => {
    const log = await fresh();
    const a = await log.append('s', 1, { namespace: 'orders', key: 'k' });
    const b = await log.append('s', 2, { namespace: 'billing', key: 'k' });
    expect(a.sequence).toBe(1);
    expect(b.sequence).toBe(2);
    await log.append('s', 1, { namespace: 'orders', key: 'k' }); // replay
    expect(log.read()).toHaveLength(2);
  });

  it('does not deduplicate keyless appends', async () => {
    const log = await fresh();
    await log.append('s', { v: 1 });
    await log.append('s', { v: 1 });
    expect(log.read()).toHaveLength(2);
  });
});

describe('concurrent same-key appends', () => {
  it('one content wins; equal contenders replay; divergent contenders conflict', async () => {
    const gate = createGate();
    const log = await EventLog.open(new GateStorage(new MemoryStorage(), gate));

    const p1 = log.append('s', { v: 'winner' }, { key: 'k' });
    const p2 = log.append('s', { v: 'winner' }, { key: 'k' });
    const p3 = log.append('s', { v: 'loser' }, { key: 'k' });

    // All three classified before the first fsync.
    await gate.blocked();
    gate.release();

    const e1 = await p1;
    const e2 = await p2;
    await expect(p3).rejects.toMatchObject({ name: 'IdempotencyConflictError' });
    expect(e1.sequence).toBe(1);
    expect(e2.sequence).toBe(1);
    expect(log.read()).toHaveLength(1);
    expect(log.conflicts()[0]!.reason).toBe('different-content');
  });

  it('handles many equal contenders under contention', async () => {
    const log = await fresh();
    const results = await Promise.all(
      Array.from({ length: 12 }, () =>
        log.append('s', { same: true, n: 1 }, { namespace: 'ns', key: 'hot' }),
      ),
    );
    expect(results.every((r) => r.sequence === 1)).toBe(true);
    expect(log.read()).toHaveLength(1);
  });
});

/** Storage wrapper whose sync() waits on an external gate. */

function createGate() {
  let resolveBlocked: () => void = () => {};
  let released = false;
  const blocked = new Promise<void>((r) => (resolveBlocked = r));
  let waiters: Array<() => void> = [];
  return {
    blocked: () => blocked,
    release() {
      released = true;
      const w = waiters;
      waiters = [];
      w.forEach((f) => f());
    },
    async enter() {
      if (waiters.length === 0) resolveBlocked();
      if (released) return;
      await new Promise<void>((r) => waiters.push(r));
    },
  };
}

class GateStorage implements LogStorage {
  constructor(
    private readonly inner: LogStorage,
    private readonly gate: ReturnType<typeof createGate>,
  ) {}
  async open() {
    const { bytes, handle } = await this.inner.open();
    const gated: LogHandle = {
      append: (data) => handle.append(data),
      sync: async () => {
        await this.gate.enter();
        await handle.sync();
      },
      truncate: (offset) => handle.truncate(offset),
      close: () => handle.close(),
    };
    return { bytes, handle: gated };
  }
}
