export type IdempotencyErrorCode =
  | 'IDEMPOTENCY_CONFLICT'
  | 'KEY_NOT_VISIBLE'
  | 'UNKNOWN_NAMESPACE'
  | 'ABORTED';

export interface ConflictDiagnostics {
  /** digest of the content that won the original append */
  existingDigest: string;
  /** digest of the rejected content */
  incomingDigest: string;
  namespace: string;
  key: string;
  /** sequence of the winning event, null when the conflict was between two requests that never produced an event */
  existingSequence: number | null;
}

export class IdempotencyConflictError extends Error {
  readonly code = 'IDEMPOTENCY_CONFLICT' as const;
  readonly existingDigest: string;
  readonly incomingDigest: string;
  readonly namespace: string;
  readonly key: string;
  readonly existingSequence: number | null;

  constructor(diag: ConflictDiagnostics) {
    super(
      `idempotency key "${diag.namespace}/${diag.key}" was already used with different content ` +
        `(existing digest ${diag.existingDigest.slice(0, 12)}…, incoming ${diag.incomingDigest.slice(0, 12)}…)`,
    );
    this.name = 'IdempotencyConflictError';
    this.existingDigest = diag.existingDigest;
    this.incomingDigest = diag.incomingDigest;
    this.namespace = diag.namespace;
    this.key = diag.key;
    this.existingSequence = diag.existingSequence;
  }
}

/**
 * The key is past its retention window (so a new use is allowed in principle)
 * but the original event is still below the all-replica visibility watermark.
 * Recycling it now could cause a duplicate to be delivered to lagging
 * replicas, so the client must retry after the watermark advances.
 */
export class KeyNotVisibleError extends Error {
  readonly code = 'KEY_NOT_VISIBLE' as const;
  readonly namespace: string;
  readonly key: string;
  readonly sequence: number;
  readonly visibilityWatermark: number;

  constructor(namespace: string, key: string, sequence: number, visibilityWatermark: number) {
    super(
      `key "${namespace}/${key}" expired before event ${sequence} became visible ` +
        `(visibility watermark ${visibilityWatermark})`,
    );
    this.name = 'KeyNotVisibleError';
    this.namespace = namespace;
    this.key = key;
    this.sequence = sequence;
    this.visibilityWatermark = visibilityWatermark;
  }
}

export class UnknownNamespaceError extends Error {
  readonly code = 'UNKNOWN_NAMESPACE' as const;
  constructor(namespace: string) {
    super(`unknown idempotency namespace "${namespace}"`);
    this.name = 'UnknownNamespaceError';
  }
}
