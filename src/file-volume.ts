import { open, type FileHandle } from 'node:fs/promises';
import type { Volume } from './volume.js';

/**
 * File-backed volume. One fd, one buffer, O_APPEND writes and fsync flush.
 * crash() drops the in-memory handle without flushing so writes still in the
 * page cache but never fsynced are modelled as lost; reopen via
 * `new FileVolume(path)` to recover.
 */
export class FileVolume implements Volume {
  readonly path: string;
  #handle: FileHandle | null = null;

  private constructor(path: string, handle: FileHandle) {
    this.path = path;
    this.#handle = handle;
  }

  static async create(path: string): Promise<FileVolume> {
    const handle = await open(path, 'a+');
    return new FileVolume(path, handle);
  }

  #fd(): FileHandle {
    if (!this.#handle) throw new Error('volume is closed; recover with FileVolume.create()');
    return this.#handle;
  }

  async append(data: Uint8Array): Promise<void> {
    // Node performs a single writev/write syscall; frames/batches smaller
    // than PIPE_BUF are atomic at the syscall boundary regardless.
    await this.#fd().appendFile(data);
  }

  async flush(): Promise<void> {
    await this.#fd().sync();
  }

  async read(): Promise<Uint8Array> {
    return this.#fd().readFile();
  }

  async truncate(size: number): Promise<void> {
    await this.#fd().truncate(size);
  }

  async crash(): Promise<void> {
    // Do not sync: anything waiting in the page cache is considered lost.
    const handle = this.#handle;
    this.#handle = null;
    if (handle) await handle.close();
  }

  async close(): Promise<void> {
    const handle = this.#handle;
    this.#handle = null;
    if (handle) await handle.sync();
    if (handle) await handle.close();
  }
}
