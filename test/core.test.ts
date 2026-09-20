import { expect, it } from 'vitest';
import { EventLog, MemoryStorage } from '../src/index.js';

it('appends in order', async () => {
  const x = await EventLog.open(new MemoryStorage());
  await x.append('a', 1);
  expect(x.read()[0]!.sequence).toBe(1);
});
