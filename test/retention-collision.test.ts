import { describe, expect, it } from 'vitest';
import { CrashDisk, EventLog, MemoryStorage, sha256Digest } from '../src/index.js';
import type { DigestFn } from '../src/index.js';

describe('retention window and visibility watermark', () => {
  it('does not recycle a fresh key even when replicas are visible', async () => {
    let t = 1000;
    const log = await EventLog.open(new MemoryStorage(), { now: () => t });
    await log.append('s', { v: 1 }, { key: 'k', retentionMs: 100 });
    await log.reportReplicaWatermark('r1', 1);
    await log.reportReplicaWatermark('r2', 1);
    expect(log.visibilityWatermark()).toBe(1);

    t += 101;
    // Expired + visible -> reusable: a different content now creates a new event.
    const second = await log.append('s', { v: 2 }, { key: 'k', retentionMs: 100 });
    expect(second.sequence).toBe(2);
    expect(log.read()).toHaveLength(2);
  });

  it('never recycles before every known replica has caught up', async () => {
    let t = 1000;
    const log = await EventLog.open(new MemoryStorage(), { now: () => t });
    await log.append('s', { warmup: true }); // seq 1, keyless
    await log.append('s', { v: 1 }, { key: 'k', retentionMs: 50 }); // seq 2, keyed
    await log.reportReplicaWatermark('r1', 2);
    await log.reportReplicaWatermark('r2', 1); // r2 lags behind the keyed event
    expect(log.visibilityWatermark()).toBe(1);

    t += 1000;
    // Long expired by wall clock, but r2 has not observed seq 2: recycling the
    // key is blocked, so reuse with different content conflicts.
    await expect(log.append('s', { v: 2 }, { key: 'k' })).rejects.toMatchObject({
      name: 'IdempotencyConflictError',
    });
    expect(log.read()).toHaveLength(2);

    await log.reportReplicaWatermark('r2', 2);
    expect(log.visibilityWatermark()).toBe(2);
    t += 1;
    const reused = await log.append('s', { v: 2 }, { key: 'k' });
    expect(reused.sequence).toBe(3);
  });

  it('never recycles before the retention window elapses', async () => {
    let t = 0;
    const log = await EventLog.open(new MemoryStorage(), { now: () => t });
    await log.append('s', { v: 1 }, { key: 'k', retentionMs: 1000 });
    await log.reportReplicaWatermark('r1', 1);
    t = 999;
    await expect(log.append('s', { v: 2 }, { key: 'k' })).rejects.toMatchObject({
      name: 'IdempotencyConflictError',
    });
    t = 1000;
    const reused = await log.append('s', { v: 2 }, { key: 'k' });
    expect(reused.sequence).toBe(2);
  });

  it('expiry state survives restart: key still protected, then reusable', async () => {
    const disk = new CrashDisk();
    let t = 0;
    let log = await EventLog.open(disk, { now: () => t });
    await log.append('s', { v: 1 }, { key: 'k', retentionMs: 100 });
    await log.reportReplicaWatermark('r1', 1);
    await log.close();

    t = 50;
    log = await EventLog.open(disk, { now: () => t });
    await expect(log.append('s', { v: 2 }, { key: 'k' })).rejects.toThrow();
    await log.close();

    t = 200;
    log = await EventLog.open(disk, { now: () => t });
    const reused = await log.append('s', { v: 2 }, { key: 'k' });
    expect(reused.sequence).toBe(2);
  });
});

describe('hash collision simulation', () => {
  // A deliberately broken digest: SHA-256 truncated so distinct contents collide.
  const collidingDigest: DigestFn = (bytes) => sha256Digest(bytes).slice(0, 4);

  it('same digest but different canonical bytes is still a conflict', async () => {
    // Birthday-bound search for two small payloads whose truncated digests agree.
    const contents: string[] = [];
    const seen = new Map<string, string>();
    let pair: [string, string] | undefined;
    for (let i = 0; i < 50_000 && !pair; i++) {
      const text = `val-${i}`;
      const d = collidingDigest(new TextEncoder().encode(JSON.stringify(text)));
      const prev = seen.get(d);
      if (prev !== undefined && prev !== text) pair = [prev, text];
      seen.set(d, text);
      contents.push(text);
    }
    expect(pair).toBeDefined();
    const [a, b] = pair!;

    const log = await EventLog.open(new MemoryStorage(), { digest: collidingDigest });
    await log.append('s', a, { key: 'k' });
    const err = await log.append('s', b, { key: 'k' }).then(
      () => null,
      (e: Error) => e,
    );
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).name).toBe('IdempotencyConflictError');
    expect(log.read()).toHaveLength(1);

    const record = log.conflicts()[0]!;
    expect(record.reason).toBe('digest-collision');
    // Digests are identical...
    expect(record.digest).toBe(record.otherDigest);
    // ...but both canonical contents are retained for diagnosis.
    expect(record.canonical).not.toEqual(record.otherCanonical);
  });

  it('identical content with identical digest still replays', async () => {
    const log = await EventLog.open(new MemoryStorage(), { digest: collidingDigest });
    const first = await log.append('s', { z: 1, a: 2 }, { key: 'k' });
    const replay = await log.append('s', { a: 2, z: 1 }, { key: 'k' });
    expect(replay.sequence).toBe(first.sequence);
    expect(log.read()).toHaveLength(1);
  });
});
