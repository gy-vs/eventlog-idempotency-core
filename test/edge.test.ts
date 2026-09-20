import { describe, expect, it } from 'vitest';
import { EventLog, MemoryStorage, CanonicalizeError } from '../src/index.js';

describe('edge', () => {
  it('non-canonicalizable item rejects the batch before any write', async () => {
    const log = await EventLog.open(new MemoryStorage());
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    await expect(
      log.appendBatch([
        { stream: 's', payload: { good: true }, key: { namespace: 'n', key: 'a' } },
        { stream: 's', payload: cyclic, key: { namespace: 'n', key: 'b' } },
      ]),
    ).rejects.toBeInstanceOf(CanonicalizeError);
    expect(log.watermark()).toBe(0);
    // claim must not be leaked: key a is still free
    const ok = await log.append('s', { good: true }, { namespace: 'n', key: 'a' });
    expect(ok.sequence).toBe(1);
  });
});
