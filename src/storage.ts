import { open as fspOpen, readFile } from 'node:fs/promises';

/**
 * A single appender on a durability log. Writes are buffered by the OS until
 * sync() resolves; a crash before sync may lose or tear the last write.
 * `append` + `sync` together define the commit boundary.
 */
export interface LogHandle {
  append(data: Uint8Array): Promise<void>;
  sync(): Promise<void>;
  /** Truncate durable data (used when replay finds a torn tail). */
  truncate(offset: number): Promise<void>;
  close(): Promise<void>;
}

export interface LogStorage {
  /** Bytes that survived the last crash (0 for fresh storage). */
  open(): Promise<{ bytes: Uint8Array; handle: LogHandle }>;
}

/** Real filesystem durability: write-through page cache, fsync for commit. */
export class FileStorage implements LogStorage {
  constructor(private readonly path: string) {}

  async open(): Promise<{ bytes: Uint8Array; handle: LogHandle }> {
    let bytes: Uint8Array;
    try {
      bytes = await readFile(this.path);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
      bytes = new Uint8Array(0);
    }
    const fh = await fspOpen(this.path, 'a+', 0o600);
    return {
      bytes,
      handle: {
        append: (data) => fh.appendFile(data),
        sync: () => fh.sync(),
        truncate: async (offset) => {
          await fh.sync();
          await fh.truncate(offset);
          await fh.sync();
        },
        close: () => fh.close(),
      },
    };
  }
}

/** Ephemeral storage (equivalent durability boundary semantics, no disk). */
export class MemoryStorage implements LogStorage {
  #data: Uint8Array<ArrayBufferLike> = new Uint8Array(0);

  async open(): Promise<{ bytes: Uint8Array; handle: LogHandle }> {
    // Hand back an immutable snapshot: later appends allocate new buffers and
    // must never mutate the bytes this recovery pass is parsing.
    const bytes = this.#data.slice();
    let alive = true;
    const self = this;
    return {
      bytes,
      handle: {
        async append(data) {
          if (!alive) throw new Error('handle closed');
          const next = new Uint8Array(self.#data.length + data.length);
          next.set(self.#data, 0);
          next.set(data, self.#data.length);
          self.#data = next;
        },
        async sync() {
          if (!alive) throw new Error('handle closed');
        },
        async truncate(offset) {
          self.#data = self.#data.subarray(0, offset);
        },
        async close() {
          alive = false;
        },
      },
    };
  }

  /** Current durable bytes (test/diagnostics). */
  snapshot(): Uint8Array {
    return this.#data;
  }
}

export interface CrashHooks {
  /**
   * Called before the bytes of a write are made durable. Return 'crash' to kill
   * the process (discarding unsynced data and invalidating the handle), or a
   * torn length for partial writeback at a crash.
   */
  beforeWrite?: (full: Uint8Array) => number | 'crash' | void;
  /** Called after fsync returns. Crashing here must keep the committed frame. */
  afterSync?: (committedBytes: number) => 'crash' | void;
}

/**
 * Crash-simulating storage. All bytes remain "volatile" until sync(); crash()
 * discards unsynced data (optionally keeping a torn prefix) and invalidates
 * every open handle, forcing recovery via a fresh open().
 */
export class CrashDisk implements LogStorage {
  #durable: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  #volatile: Uint8Array<ArrayBufferLike> | null = null;
  #hooks: CrashHooks;
  #open = false;
  /** Faults fire once: reopening models a recovered process with healthy I/O. */
  #beforeWriteFired = false;
  #afterSyncFired = false;

  constructor(hooks: CrashHooks = {}) {
    this.#hooks = hooks;
  }

  crash(tornBytes = 0): void {
    if (this.#volatile !== null) {
      // Copy: a lingering view from before the crash must not alias bytes the
      // recovered process writes later.
      this.#durable = this.#volatile.slice(0, Math.min(tornBytes, this.#volatile.length));
    }
    this.#volatile = null;
    this.#open = false;
  }

  async open(): Promise<{ bytes: Uint8Array; handle: LogHandle }> {
    // Recovery snapshot must not alias the buffer later appends write into.
    this.#volatile = this.#durable.slice();
    this.#open = true;
    const startLen = this.#durable.length;
    const disk = this;
    const handle: LogHandle = {
      async append(data) {
        if (!disk.#open || disk.#volatile === null) throw new Error('disk crashed: handle invalid');
        const v = disk.#volatile;
        const full = new Uint8Array(v.length + data.length);
        full.set(v, 0);
        full.set(data, v.length);
        if (disk.#hooks.beforeWrite && !disk.#beforeWriteFired) {
          disk.#beforeWriteFired = true;
          const r = disk.#hooks.beforeWrite(full.subarray(v.length));
          if (r === 'crash') {
            disk.crash(0);
            throw new Error('simulated crash before writeback');
          }
          if (typeof r === 'number') {
            disk.crash(startLen + Math.max(0, r));
            throw new Error('simulated crash during writeback');
          }
        }
        disk.#volatile = full;
      },
      async sync() {
        if (!disk.#open || disk.#volatile === null) throw new Error('disk crashed: handle invalid');
        const committed = disk.#volatile;
        disk.#durable = committed;
        if (disk.#hooks.afterSync && !disk.#afterSyncFired) {
          disk.#afterSyncFired = true;
          const r = disk.#hooks.afterSync(disk.#durable.length);
          if (r === 'crash') {
            // fsync already returned: durable bytes survive, only the process dies.
            disk.#volatile = null;
            disk.#open = false;
            throw new Error('simulated process crash immediately after fsync');
          }
        }
      },
      async truncate(offset) {
        if (!disk.#open || disk.#volatile === null) throw new Error('disk crashed: handle invalid');
        const cut = Math.min(offset, disk.#volatile.length);
        disk.#volatile = disk.#volatile.slice(0, cut);
        disk.#durable = disk.#volatile;
      },
      async close() {
        if (disk.#open && disk.#volatile !== null) disk.#durable = disk.#volatile.slice();
        disk.#volatile = null;
        disk.#open = false;
      },
    };
    return { bytes: this.#durable.slice(), handle };
  }
}
