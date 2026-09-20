import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { EventLog, FileStorage } from '../src/index.js';

describe('FileStorage integration', () => {
  let dir: string;

  beforeAll(async () => {
    dir = await mkdtemp(join(tmpdir(), 'eventlog-'));
  });
  afterAll(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it('persists across real process-equivalent reopen with fsync', async () => {
    const path = join(dir, 'log.bin');
    let log = await EventLog.open(new FileStorage(path));
    await log.append('orders', { id: 42, amount: 7 }, { namespace: 'ns', key: 'k' });
    await log.append('orders', { id: 43 }, { namespace: 'ns', key: 'k2' });
    await expect(
      log.append('orders', { id: 99 }, { namespace: 'ns', key: 'k' }),
    ).rejects.toThrow(/IdempotencyConflictError|idempotency conflict/);
    await log.reportReplicaWatermark('r1', 2);
    await log.close();

    log = await EventLog.open(new FileStorage(path));
    expect(log.watermark()).toBe(2);
    expect(log.visibilityWatermark()).toBe(2);
    const replay = await log.append('orders', { amount: 7, id: 42 }, {
      namespace: 'ns',
      key: 'k',
    });
    expect(replay.sequence).toBe(1);
    expect(log.read()).toHaveLength(2);
    expect(log.conflicts('ns', 'k')).toHaveLength(1);
    await log.close();
  });
});
