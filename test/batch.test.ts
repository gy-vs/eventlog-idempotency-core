import { describe, expect, it } from 'vitest';
import type { LogHandle, LogStorage } from '../src/index.js';
import { CrashDisk, EventLog, IdempotencyConflictError, MemoryStorage } from '../src/index.js';

describe('batch append', () => {
  it('commits a whole keyed batch in one atomic unit with contiguous sequences', async () => {
    const log = await EventLog.open(new MemoryStorage());
    const outcomes = await log.appendBatch([
      { stream: 's', payload: 1, key: { namespace: 'ns', key: 'a' } },
      { stream: 's', payload: 2 },
      { stream: 's', payload: 3, key: { namespace: 'ns', key: 'b' } },
    ]);
    expect(outcomes.map((o) => o.status)).toEqual(['created', 'created', 'created']);
    expect(outcomes.map((o) => (o.status === 'conflict' ? null : o.event.sequence))).toEqual([
      1, 2, 3,
    ]);
    expect(log.read()).toHaveLength(3);
  });

  it('partial duplicates replay without new events; new items still commit', async () => {
    const log = await EventLog.open(new MemoryStorage());
    await log.append('s', { v: 1 }, { namespace: 'ns', key: 'a' });
    await log.append('s', { v: 2 }, { namespace: 'ns', key: 'b' });

    const outcomes = await log.appendBatch([
      { stream: 's', payload: { v: 1 }, key: { namespace: 'ns', key: 'a' } }, // replay
      { stream: 's', payload: { v: 3 }, key: { namespace: 'ns', key: 'c' } }, // created
      { stream: 's', payload: { v: 2 }, key: { namespace: 'ns', key: 'b' } }, // replay
    ]);

    expect(outcomes.map((o) => o.status)).toEqual(['replayed', 'created', 'replayed']);
    expect(outcomes[0]!.status !== 'conflict' && outcomes[0]!.event.sequence).toBe(1);
    expect(outcomes[1]!.status !== 'conflict' && outcomes[1]!.event.sequence).toBe(3);
    expect(outcomes[2]!.status !== 'conflict' && outcomes[2]!.event.sequence).toBe(2);
    expect(log.read()).toHaveLength(3);
  });

  it('conflicting items are reported per position and never written', async () => {
    const log = await EventLog.open(new MemoryStorage());
    await log.append('s', { v: 1 }, { key: 'k' });

    const outcomes = await log.appendBatch([
      { stream: 's', payload: { v: 1 }, key: { namespace: 'default', key: 'k' } },
      { stream: 's', payload: { v: 9 }, key: { namespace: 'default', key: 'k' } },
      { stream: 's', payload: { fresh: true }, key: { namespace: 'default', key: 'k2' } },
    ]);

    expect(outcomes[0]!.status).toBe('replayed');
    expect(outcomes[1]!.status).toBe('conflict');
    expect(outcomes[2]!.status).toBe('created');
    if (outcomes[1]!.status === 'conflict') {
      expect(outcomes[1]!.error).toBeInstanceOf(IdempotencyConflictError);
      expect(outcomes[1]!.error.diagnostics.incomingDigest).not.toBe(
        outcomes[1]!.error.diagnostics.storedDigest,
      );
    }
    if (outcomes[2]!.status !== 'conflict') expect(outcomes[2]!.event.sequence).toBe(2);
    expect(log.read().map((e) => e.sequence)).toEqual([1, 2]);
    expect(log.conflicts()).toHaveLength(1);
  });

  it('repeated batch retries stay idempotent', async () => {
    const log = await EventLog.open(new MemoryStorage());
    const batch = [
      { stream: 's', payload: { id: 1 }, key: { namespace: 'ns', key: 'x' } },
      { stream: 's', payload: { id: 2 }, key: { namespace: 'ns', key: 'y' } },
    ];
    const first = await log.appendBatch(batch);
    const second = await log.appendBatch(batch);
    expect(second.map((o) => (o.status === 'conflict' ? null : o.event.sequence))).toEqual(
      first.map((o) => (o.status === 'conflict' ? null : o.event.sequence)),
    );
    expect(second.every((o) => o.status === 'replayed')).toBe(true);
    expect(log.read()).toHaveLength(2);
  });

  it('log events and key registrations share a single fsync boundary', async () => {
    const counts = { writes: 0, syncs: 0 };
    const counting: LogStorage = {
      async open() {
        const mem = await new MemoryStorage().open();
        const handle: LogHandle = {
          append: async (data) => {
            counts.writes += 1;
            await mem.handle.append(data);
          },
          sync: async () => {
            counts.syncs += 1;
            await mem.handle.sync();
          },
          truncate: (offset) => mem.handle.truncate(offset),
          close: () => mem.handle.close(),
        };
        return { bytes: mem.bytes, handle };
      },
    };

    const log = await EventLog.open(counting);
    await log.appendBatch([
      { stream: 's', payload: 1, key: { namespace: 'ns', key: 'a' } },
      { stream: 's', payload: 2, key: { namespace: 'ns', key: 'b' } },
      { stream: 's', payload: 3 },
    ]);
    expect(counts).toEqual({ writes: 1, syncs: 1 });
  });

  it('same key repeated within one batch: first wins, equal replays, divergent conflicts', async () => {
    const log = await EventLog.open(new MemoryStorage());
    const outcomes = await log.appendBatch([
      { stream: 's', payload: { v: 1 }, key: { namespace: 'ns', key: 'k' } },
      { stream: 's', payload: { v: 1 }, key: { namespace: 'ns', key: 'k' } },
      { stream: 's', payload: { v: 2 }, key: { namespace: 'ns', key: 'k' } },
      { stream: 's', payload: { other: true } },
    ]);
    expect(outcomes.map((o) => o.status)).toEqual([
      'created',
      'replayed',
      'conflict',
      'created',
    ]);
    expect(outcomes[0]!.status !== 'conflict' && outcomes[0]!.event.sequence).toBe(1);
    expect(outcomes[1]!.status !== 'conflict' && outcomes[1]!.event.sequence).toBe(1);
    expect(outcomes[3]!.status !== 'conflict' && outcomes[3]!.event.sequence).toBe(2);
    expect(log.read()).toHaveLength(2);
    expect(log.conflicts()).toHaveLength(1);
  });

  it('atomic boundary: a failed fsync commits nothing from the batch', async () => {
    const disk = new CrashDisk({
      beforeWrite: () => 'crash',
    });
    let log = await EventLog.open(disk);
    await expect(
      log.appendBatch([
        { stream: 's', payload: 1, key: { namespace: 'ns', key: 'a' } },
        { stream: 's', payload: 2, key: { namespace: 'ns', key: 'b' } },
      ]),
    ).rejects.toThrow();

    log = await EventLog.open(disk);
    expect(log.watermark()).toBe(0);
    // Both keys are free again — no phantom registrations.
    const outcomes = await log.appendBatch([
      { stream: 's', payload: 1, key: { namespace: 'ns', key: 'a' } },
      { stream: 's', payload: 2, key: { namespace: 'ns', key: 'b' } },
    ]);
    expect(outcomes.map((o) => o.status)).toEqual(['created', 'created']);
  });
});
