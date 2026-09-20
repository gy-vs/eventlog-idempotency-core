# Event log core

Append-only event storage with idempotent append keys.

Run `npm install`, then `npm test` and `npm run build`.

## Idempotent append

`append(stream, payload, { namespace, key, retentionMs })` attaches an
idempotency key to an append:

- **Same key, same content** (retries after timeout, duplicate delivery) returns
  the original sequence and never writes a second event.
- **Same key, different content** throws `IdempotencyConflictError` and writes
  nothing. Both content digests are persisted and available from
  `log.conflicts(namespace, key)` for diagnostics.
- Content equality is based on a deterministic encoding
  (`canonicalize`): plain JSON objects, keys sorted by UTF-16 code unit, arrays
  in order — object field order cannot change the result. Digests are SHA-256
  over those bytes, and equal digest plus full byte comparison means a digest
  collision can never silently alias two different payloads.
- Keys are scoped by `namespace` (default `"default"`).

`appendBatch(items)` appends a batch atomically and returns a per-item outcome
(`created` / `replayed` / `conflict`), so a retried batch with a mix of new and
duplicate keys only writes the new items once.

## Commit boundary and crash recovery

There is no separate idempotency index on disk. Every commit is one checksummed
length-prefixed frame containing the batch's events **and** its key
registrations, made durable with a single `write` + `fsync`:

```
magic | uint32 length | JSON payload | SHA-256 checksum
```

The in-memory index is purely a materialization of that log. On open the log is
replayed frame by frame, the index and conflict history are rebuilt, and any
torn tail from a crash mid-writeback is detected via checksum and truncated.
Therefore "event written but index not written" is impossible: after a crash
both are present together, or neither. In-flight state that never reached
fsync is discarded and the same key can append again after recovery.

Pluggable storage (`LogStorage`):

- `FileStorage(path)` — real filesystem durability (`fsync`).
- `MemoryStorage` — ephemeral, same boundary semantics.
- `CrashDisk` — test double that models crashes before/after fsync and partial
  (torn) writeback, invalidating handles like a dead process.

## Retention and visibility

A key is reusable only when **both** conditions hold at append time:

1. its retention window (`retentionMs`, default 24h, per key) has elapsed, and
2. its sequence is at or below the all-replica **visibility watermark**.

Replicas report their contiguous observed sequence via
`reportReplicaWatermark(replica, seq)`; the visibility watermark is the minimum
across every known replica (also persisted in the log). A lagging replica can
never cause an entry it has not seen to be recycled.

## Concurrency

Concurrent appends carrying the same key are classified synchronously before
any await: exactly one content becomes the winner and owns the commit; the
other callers wait on that claim. Equal-content waiters return the winner's
sequence; different-content waiters receive a conflict. A failed owner commit
aborts its waiters so every caller retries cleanly. Physical commits are
serialized through an internal chain.

## Layout

- `src/canonical.ts` — deterministic encoding + digest
- `src/frame.ts` — on-disk frame codec and checksummed replay
- `src/storage.ts` — `FileStorage`, `MemoryStorage`, `CrashDisk`
- `src/errors.ts` — `IdempotencyConflictError` with both digests
- `src/index.ts` — `EventLog`: append/batch, index, recovery, retention/GC
