import { privacyGenerationHealthy } from '../metrics.js';

export const PRIVACY_GENERATION_TIMEOUT_MS = 2_000;

export type PrivacyGenerationSnapshot = {
  desiredGeneration: string | null;
  materializedGeneration: string | null;
  appliedGeneration: string | null;
  queueActive: boolean | null;
};

// One MVCC snapshot, two singleton lookups and an indexed EXISTS. No packet,
// prefix or historical artefact scans. "Applied" is the generation admissible
// to the global public-read gate, not a separate downstream consumer cursor.
export const PRIVACY_GENERATION_HEALTH_SQL = `
  WITH state AS (
    SELECT
      (SELECT generation::text FROM public_visibility_state WHERE singleton = TRUE) AS desired,
      (SELECT visibility_generation::text FROM packet_visibility_materialization_state
        WHERE singleton = TRUE) AS materialized,
      EXISTS (SELECT 1 FROM privacy_rematerialization_queue
        WHERE status IN ('pending', 'processing', 'failed')) AS queue_active
  )
  SELECT desired AS "desiredGeneration", materialized AS "materializedGeneration",
         CASE WHEN desired = materialized AND NOT queue_active
           THEN materialized ELSE NULL END AS "appliedGeneration",
         queue_active AS "queueActive"
    FROM state
`;

type Reason = 'healthy' | 'desired_missing' | 'materialized_missing' | 'applied_missing'
  | 'invalid_generation' | 'desired_ahead' | 'materialized_ahead'
  | 'applied_ahead' | 'applied_behind' | 'queue_active' | 'read_failed';
export type PrivacyGenerationHealth = PrivacyGenerationSnapshot & {
  healthy: boolean;
  reason: Reason;
};

function validGeneration(value: string | null): value is string {
  return typeof value === 'string' && /^[1-9][0-9]{0,15}$/.test(value)
    && BigInt(value) <= BigInt(Number.MAX_SAFE_INTEGER);
}

export function evaluatePrivacyGenerations(state: PrivacyGenerationSnapshot): PrivacyGenerationHealth {
  const { desiredGeneration: desired, materializedGeneration: materialized, appliedGeneration: applied } = state;
  let reason: Reason;
  if (desired === null) reason = 'desired_missing';
  else if (materialized === null) reason = 'materialized_missing';
  else if (!validGeneration(desired) || !validGeneration(materialized)
    || (applied !== null && !validGeneration(applied))) reason = 'invalid_generation';
  else if (BigInt(desired) > BigInt(materialized)) reason = 'desired_ahead';
  else if (BigInt(materialized) > BigInt(desired)) reason = 'materialized_ahead';
  else if (state.queueActive !== false) reason = 'queue_active';
  else if (applied === null) reason = 'applied_missing';
  else if (BigInt(applied) > BigInt(desired)) reason = 'applied_ahead';
  else if (BigInt(applied) < BigInt(desired)) reason = 'applied_behind';
  else reason = 'healthy';
  return { ...state, healthy: reason === 'healthy', reason };
}

function unavailable(): PrivacyGenerationHealth {
  return {
    desiredGeneration: null, materializedGeneration: null, appliedGeneration: null,
    queueActive: null, healthy: false, reason: 'read_failed',
  };
}

function publish(state: PrivacyGenerationHealth): void {
  privacyGenerationHealthy.set(state.healthy ? 1 : 0);
  // Fixed fields only: no SQL, dependency addresses, identities or exceptions.
  const line = JSON.stringify({ event: 'privacy_generation_health', ...state });
  if (state.healthy) console.log(line);
  else console.warn(line);
}

/** Read-only observation, shared by health and scrapes. At most one underlying
 * read can be pending, including after timeout while pool acquisition drains.
 * The DB query adapter must honor abort (the application's query() does).
 */
export function createPrivacyGenerationReconciler(
  read: (signal: AbortSignal) => Promise<{ rows: PrivacyGenerationSnapshot[] }>,
  report: (state: PrivacyGenerationHealth) => void = publish,
): () => Promise<PrivacyGenerationHealth> {
  let inflight: Promise<PrivacyGenerationHealth> | null = null;
  return () => {
    if (inflight) return inflight;
    const controller = new AbortController();
    const pending = Promise.resolve().then(() => read(controller.signal));
    let timer: ReturnType<typeof setTimeout>;
    const bounded = new Promise<PrivacyGenerationHealth>((resolve) => {
      timer = setTimeout(() => {
        controller.abort();
        resolve(unavailable());
      }, PRIVACY_GENERATION_TIMEOUT_MS);
      void pending.then(
        ({ rows }) => resolve(rows[0] ? evaluatePrivacyGenerations(rows[0]) : unavailable()),
        () => resolve(unavailable()),
      );
    });
    const tracked = bounded.then((state) => {
      report(state);
      return state;
    }).finally(() => clearTimeout(timer));
    inflight = tracked;
    void Promise.allSettled([pending, tracked]).then(() => {
      if (inflight === tracked) inflight = null;
    });
    return tracked;
  };
}
