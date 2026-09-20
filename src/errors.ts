export interface ConflictDiagnostics {
  namespace: string;
  key: string;
  /** Digest of the content that originally won the key. */
  storedDigest: string;
  /** Digest of the rejected duplicate content. */
  incomingDigest: string;
  /** Sequence of the winning record (if committed); undefined for a live winner. */
  storedSequence?: number;
  /** Wall-clock time at which the conflict was detected (ms epoch). */
  detectedAt: number;
}

/**
 * Raised when an idempotency key is reused with different content. Both content
 * digests are retained for diagnostics; the call does not append anything.
 */
export class IdempotencyConflictError extends Error {
  readonly diagnostics: ConflictDiagnostics;

  constructor(diagnostics: ConflictDiagnostics) {
    super(
      `idempotency conflict for key "${diagnostics.namespace}/${diagnostics.key}": ` +
        `stored digest ${diagnostics.storedDigest.slice(0, 12)} vs incoming ${diagnostics.incomingDigest.slice(0, 12)}`,
    );
    this.name = 'IdempotencyConflictError';
    this.diagnostics = diagnostics;
  }
}
