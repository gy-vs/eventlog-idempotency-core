import { describe, expect, it } from 'vitest';
import type { LogHandle, LogStorage } from '../src/index.js';
import { CrashDisk, EventLog } from '../src/index.js';

/** Delays every fsync so the client can time out while a call is in flight. */
class SlowSyncStorage implements LogStorage {
  constructor(
    private readonly inner: LogStorage,
    private readonly delayMs: number,
  ) {}
  async open() {
    const { bytes, handle } = await this.inner.open();
    const slow: LogHandle = {
      append: (data) => handle.append(data),
      sync: () =>
        new Promise<void>((resolve) => setTimeout(() => handle.sync().then(resolve), this.delayMs)),
      truncate: (offset) => handle.truncate(offset),
      close: () => handle.close(),
    };
    return { bytes, handle: slow };
  }
}

/**
 * Models write-back buffering: appended bytes stay uncommitted until sync()
 * succeeds; a failed sync discards them. The first sync fails transiently, the
 * process and handle stay alive.
 */
class FlakySyncStorage implements LogStorage {
  #committed = new Uint8Array(0);
  #pending = new Uint8Array(0);
  #alive = true;
  failed = false;

  constructor(_inner?: LogStorage) {}

  async open() {
    const self = this;
    const handle: LogHandle = {
      async append(data) {
        const next = new Uint8Array(self.#pending.length + data.length);
        next.set(self.#pending, 0);
        next.set(data, self.#pending.length);
        self.#pending = next;
      },
      async sync() {
        if (!self.#alive) throw new Error('handle closed');
        if (!self.failed) {
          self.failed = true;
          self.#pending = new Uint8Array(0); // unsynced write lost
          throw new Error('EIO: transient fsync failure');
        }
        self.#committed = self.#pending;
      },
      async truncate(offset) {
        self.#pending = self.#pending.subarray(0, Math.min(offset, self.#pending.length));
      },
      async close() {
        self.#alive = false;
      },
    };
    return { bytes: this.#committed.slice(), handle };
  }
}

describe('client timeout then retry', () => {
  it('returns the original sequence without a second event', async () => {
    const disk = new CrashDisk();
    const openLog = () =>
      EventLog.open(new SlowSyncStorage(disk, 30), { now: () => 1000 });

    let log = await openLog();
    const first = log.append('orders', { amount: 5 }, { namespace: 'ns', key: 'order-7' });
    // Client gives up while fsync is still running...
    await expect(Promise.race([first, wait(10)])).resolves.toBeUndefined();
    // ...but the server still completes the commit.
    await first;

    // Client retries the same key+content on a fresh attempt.
    const retry = await log.append('orders', { amount: 5 }, { namespace: 'ns', key: 'order-7' });
    expect(retry.sequence).toBe(1);
    expect(log.read()).toHaveLength(1);
  });

  it('a divergent retry after timeout is a conflict with both digests', async () => {
    const disk = new CrashDisk();
    const openLog = () => EventLog.open(new SlowSyncStorage(disk, 30));

    let log = await openLog();
    const first = log.append('s', { v: 1 }, { key: 'k' });
    await wait(10); // timed out on the client side
    await first; // server finished

    await expect(log.append('s', { v: 2 }, { key: 'k' })).rejects.toMatchObject({
      name: 'IdempotencyConflictError',
    });
    expect(log.read()).toHaveLength(1);
    expect(log.conflicts()).toHaveLength(1);
  });
});

describe('crash around fsync', () => {
  it('crash before fsync: nothing durable; same key can append once after recovery', async () => {
    const disk = new CrashDisk({
      beforeWrite: () => 'crash',
    });
    let log = await EventLog.open(disk);
    await expect(
      log.append('s', { v: 1 }, { namespace: 'ns', key: 'k' }),
    ).rejects.toThrow(/crash before writeback/);

    // New process, recovered log: no event, no index entry.
    log = await EventLog.open(disk);
    expect(log.watermark()).toBe(0);
    const again = await log.append('s', { v: 1 }, { namespace: 'ns', key: 'k' });
    expect(again.sequence).toBe(1);
    expect(log.read()).toHaveLength(1);
  });

  it('crash immediately after fsync: event AND key survive together', async () => {
    const disk = new CrashDisk({
      afterSync: () => 'crash',
    });
    let log = await EventLog.open(disk);
    await expect(
      log.append('s', { v: 1 }, { namespace: 'ns', key: 'k' }),
    ).rejects.toThrow(/immediately after fsync/);

    log = await EventLog.open(disk);
    expect(log.read()).toHaveLength(1);
    // Index was rebuilt from the log: the retry replays, no duplicate event.
    const retry = await log.append('s', { v: 1 }, { namespace: 'ns', key: 'k' });
    expect(retry.sequence).toBe(1);
    expect(log.watermark()).toBe(1);
  });

  it('torn tail from partial writeback is truncated on recovery', async () => {
    // First commit lands fully; the second write is torn halfway through.
    let crashed = false;
    const disk = new CrashDisk({
      beforeWrite: (data) => {
        if (!crashed) {
          crashed = true;
          return Math.floor(data.length / 2);
        }
      },
    });
    let log = await EventLog.open(disk);
    await expect(log.append('s', { v: 'second' }, { key: 'k2' })).rejects.toThrow(
      /during writeback/,
    );

    log = await EventLog.open(disk);
    // Recovery must not surface a half-written event or registration.
    expect(log.read()).toEqual([]);
    const replaced = await log.append('s', { v: 'second' }, { key: 'k2' });
    expect(replaced.sequence).toBe(1);
  });

  it('concurrent waiters on a failed commit are aborted and can retry once', async () => {
    let failedOnce = false;
    const disk = new CrashDisk({
      beforeWrite: () => {
        if (!failedOnce) {
          failedOnce = true;
          return 'crash';
        }
      },
    });
    let log = await EventLog.open(disk);
    const owner = log.append('s', { v: 1 }, { key: 'k' });
    const waiter = log.append('s', { v: 1 }, { key: 'k' });
    await expect(owner).rejects.toThrow();
    await expect(waiter).rejects.toThrow();

    // Both calls failed before the commit boundary; recovered process retries.
    log = await EventLog.open(disk);
    const retry = await log.append('s', { v: 1 }, { key: 'k' });
    expect(retry.sequence).toBe(1);
    expect(log.read()).toHaveLength(1);
  });

  it('failed fsync leaves no phantom events or consumed sequences in the live instance', async () => {
    const storage = new FlakySyncStorage();
    const log = await EventLog.open(storage);
    await expect(log.append('s', { v: 1 }, { key: 'k' })).rejects.toThrow(/transient fsync/);
    // Nothing observable from the failed commit — even without reopening.
    expect(log.watermark()).toBe(0);
    expect(log.read()).toEqual([]);
    expect(log.conflicts()).toEqual([]);

    // Retry in the SAME process/instance after I/O recovers: starts at seq 1.
    const retry = await log.append('s', { v: 1 }, { key: 'k' });
    expect(retry.sequence).toBe(1);
    expect(log.watermark()).toBe(1);
  });
});

describe('index rebuild', () => {
  it('rebuilds the idempotency index and conflict history purely from the log', async () => {
    const disk = new CrashDisk();
    let log = await EventLog.open(disk);
    await log.append('s', { v: 1 }, { namespace: 'ns', key: 'a' });
    await log.append('s', { v: 2 }, { namespace: 'ns', key: 'b' });
    await expect(log.append('s', { v: 99 }, { namespace: 'ns', key: 'a' })).rejects.toThrow();
    await log.reportReplicaWatermark('r1', 2);
    await log.close();

    // Fresh process: only the log bytes exist.
    log = await EventLog.open(disk);
    expect(log.watermark()).toBe(2);
    expect(log.visibilityWatermark()).toBe(2);
    const replay = await log.append('s', { v: 1 }, { namespace: 'ns', key: 'a' });
    expect(replay.sequence).toBe(1);
    expect(log.read()).toHaveLength(2);
    expect(log.conflicts('ns', 'a')).toHaveLength(1);
    expect(log.conflicts('ns', 'a')[0]!.otherDigest).toBeTruthy();
  });
});

function wait(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}
