/**
 * Storage primitives behind the durable log.
 *
 * append() may remain in an OS/page buffer; flush() is the durability point
 * (fsync for files). crash() drops everything that has not been flushed,
 * modelling a power loss. truncate() is only used during recovery, to remove
 * a torn tail physically.
 *
 * After crash() the instance is unusable; recovery tests construct a fresh
 * volume over the same durable state (a path for files, snapshotBytes() for
 * the memory volume).
 */
export interface Volume {
  append(data: Uint8Array): void | Promise<void>;
  flush(): void | Promise<void>;
  read(): Uint8Array | Promise<Uint8Array>;
  truncate(size: number): void | Promise<void>;
  /** simulate a crash: close descriptors and discard unflushed writes */
  crash(): void | Promise<void>;
  close(): void | Promise<void>;
}

/**
 * In-memory volume for tests. `bytes` holds flushed state, `pending` holds
 * writes since the last flush; crash() discards `pending` and freezes the
 * instance. Reads expose flushed state only — unflushed writes are never
 * observable, matching durability semantics (the log always flushes before
 * exposing a result during normal operation).
 */
export class MemoryVolume implements Volume {
  #bytes: Uint8Array;
  #pending = new Uint8Array(0);
  #crashed = false;
  #closed = false;

  constructor(initial: Uint8Array = new Uint8Array(0)) {
    this.#bytes = initial.slice();
  }

  /** durable (flushed) state — feed it to a new MemoryVolume to "reopen" */
  snapshotBytes(): Uint8Array {
    return this.#bytes.slice();
  }

  append(data: Uint8Array): void {
    this.#checkOpen();
    const next = new Uint8Array(this.#pending.length + data.length);
    next.set(this.#pending);
    next.set(data, this.#pending.length);
    this.#pending = next;
  }

  flush(): void {
    this.#checkOpen();
    if (this.#pending.length) {
      const next = new Uint8Array(this.#bytes.length + this.#pending.length);
      next.set(this.#bytes);
      next.set(this.#pending, this.#bytes.length);
      this.#bytes = next;
      this.#pending = new Uint8Array(0);
    }
  }

  read(): Uint8Array {
    return this.#bytes.slice();
  }

  truncate(size: number): void {
    this.#checkOpen();
    this.#bytes = this.#bytes.subarray(0, Math.min(size, this.#bytes.length)).slice();
  }

  crash(): void {
    this.#crashed = true;
    this.#pending = new Uint8Array(0);
  }

  close(): void {
    this.#closed = true;
  }

  #checkOpen(): void {
    if (this.#crashed) throw new Error('volume has crashed; recover with a new instance');
    if (this.#closed) throw new Error('volume is closed');
  }
}
