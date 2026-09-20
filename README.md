# Event log core

TypeScript library for append-only event storage with **idempotent appends**.

Run `npm install`, then `npm test` and `npm run build`.

## Why idempotency

Clients retry on timeout. Every keyed append takes:

- a `namespace`
- an idempotency `key`
- the request `payload`

Behavior:

| Same key, same content | same sequence, `replay: true` — no second event is written |
| Same key, different content | `IdempotencyConflictError`; **both content digests are retained** for diagnostics (`getConflict`) |
| Key unknown / recycled | a new event is appended |

Content equality is based on a deterministic canonical encoding
(`canonicalize`): object keys are sorted recursively, so
`{a:1,b:2}` and `{b:2,a:1}` are the same content, while array order and type
differences are significant. Canonical bytes are UTF-8 and hashed with
SHA-256 (the hasher is injectable for testing). If two different canonical
byte strings ever produce the same digest, byte comparison still rejects the
replay — a hash collision is reported as a conflict, never as a duplicate.

## Usage

```ts
const log = await EventLog.open({
  volume: await FileVolume.create('/var/log/events.bin'),
  namespaces: { default: 86_400_000, payments: 3_600_000 }, // retention ms
});

const result = await log.append('orders', { amount: 100 }, { key: 'req-7' });
// result.sequence === 1, result.replay === false

const retry = await log.append('orders', { amount: 100 }, { key: 'req-7' });
// retry.sequence === 1, retry.replay === true — log still has one event
```

Client timeouts are expressed with an `AbortSignal`. A signal that fires while
the append is queued prevents the append from ever running. A signal that
fires during the fsync **does not roll the commit back**: the retry must be
able to observe the original sequence. The abandoned caller receives its
abort error once the commit completes.

Batches (`appendMany`) are all-or-nothing, including their idempotency
records: duplicate keys inside one batch collapse onto one event (equal
content) or fail the entire batch (different content).

## Shared commit boundary and crash recovery

The log is a sequence of length-prefixed, CRC-32-checked frames:

```
BATCH, EVENT…, [CONFLICT…], [WATERMARK], COMMIT
```

- One `BATCH`/`COMMIT` group is one commit boundary, ending in a single
  `flush()` (fsync). Idempotency key information lives **inside the same event
  frame** as the event; conflict diagnostics and watermark updates are frames
  in the same group.
- The in-memory idempotency index is mutated only **after** the flush returns,
  so it can never point at an event that is not durable.
- On open, the log scans frames, checks CRCs, applies only complete groups
  (everything after the last `COMMIT` is discarded) and truncates any torn
  tail. The index, conflict diagnostics and visibility watermark are rebuilt
  entirely from the log — there is no separate index file that could disagree
  with it. "Log written, index missing" therefore cannot produce a duplicate.

## Retention windows and the all-replica visibility watermark

Each namespace configures a retention window. Index entries carry an
`expiresAt` timestamp. An entry is removed only when **both** hold:

1. `expiresAt <= now`, and
2. `entry.sequence <= visibilityWatermark`

`advanceVisibilityWatermark(seq)` reports the highest sequence visible on
every replica and persists it in the log. Reusing an expired key whose
original event is still above the watermark fails with `KeyNotVisibleError`
rather than risking a duplicate delivery to lagging replicas.

`autoVisible: true` advances the watermark to the new tail inside every
append's own commit group (useful for single-replica deployments).

## Volumes

- `FileVolume` — real durability (`O_APPEND` writes + `fsync`).
- `MemoryVolume` — tests. `snapshotBytes()` + `crash()` model power loss
  before/after fsync; reopen with a new volume over the snapshot.
