import type { Pool, PoolClient, QueryResultRow } from 'pg';

// One cluster-wide session advisory lock serialises the three analytical jobs
// that have the highest database hold time. PostgreSQL owns the claim, so a
// crashed process or severed connection releases it without a stale lock row.
const HEAVY_WORK_LOCK_CLASS = 1_433_214_840;
const HEAVY_WORK_LOCK_KEY = 1_463_235_684;
const DEFAULT_RETRY_MS = 1_000;

type AdmissionClient = Pick<PoolClient, 'query' | 'release'>;
type AdmissionPool = Pick<Pool, 'connect'>;

type HeavyWorkAdmissionOptions<T> = {
  pool: AdmissionPool;
  workload: string;
  task: () => Promise<T>;
  signal?: AbortSignal;
  retryMs?: number;
  log?: Pick<Console, 'log' | 'warn'>;
};

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(signal.reason ?? new Error('HEAVY_WORK_ADMISSION_ABORTED'));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason ?? new Error('HEAVY_WORK_ADMISSION_ABORTED'));
    };
    signal?.addEventListener('abort', onAbort, { once: true });
    if (signal) {
      const cleanup = () => signal.removeEventListener('abort', onAbort);
      setTimeout(cleanup, ms);
    }
  });
}

async function tryAcquire(client: AdmissionClient): Promise<boolean> {
  const result = await client.query<{ acquired: boolean }>(
    'SELECT pg_try_advisory_lock($1, $2) AS acquired',
    [HEAVY_WORK_LOCK_CLASS, HEAVY_WORK_LOCK_KEY],
  );
  return result.rows[0]?.acquired === true;
}

export async function withHeavyWorkAdmission<T>(
  options: HeavyWorkAdmissionOptions<T>,
): Promise<T> {
  if (!/^[a-z0-9][a-z0-9:_-]{0,95}$/i.test(options.workload)) {
    throw new Error('INVALID_HEAVY_WORKLOAD_NAME');
  }
  const retryMs = Math.max(1, Math.min(30_000, Math.trunc(options.retryMs ?? DEFAULT_RETRY_MS)));
  const logger = options.log ?? console;
  const client = await options.pool.connect() as AdmissionClient;
  const waitingStartedAt = Date.now();
  let acquired = false;
  let waitingLogged = false;
  try {
    while (!acquired) {
      options.signal?.throwIfAborted();
      acquired = await tryAcquire(client);
      if (acquired) break;
      if (!waitingLogged) {
        logger.log(`[db-admission] workload=${options.workload} waiting`);
        waitingLogged = true;
      }
      await sleep(retryMs, options.signal);
    }
    const acquiredAt = Date.now();
    logger.log(
      `[db-admission] workload=${options.workload} acquired waitMs=${acquiredAt - waitingStartedAt}`,
    );
    try {
      return await options.task();
    } finally {
      const unlock = await client.query<QueryResultRow>(
        'SELECT pg_advisory_unlock($1, $2) AS unlocked',
        [HEAVY_WORK_LOCK_CLASS, HEAVY_WORK_LOCK_KEY],
      ).catch((error: unknown) => {
        logger.warn(
          `[db-admission] workload=${options.workload} unlock failed: ${error instanceof Error ? error.message : String(error)}`,
        );
        return null;
      });
      const unlocked = unlock?.rows[0]?.['unlocked'] === true;
      logger.log(
        `[db-admission] workload=${options.workload} released durationMs=${Date.now() - acquiredAt} unlocked=${unlocked}`,
      );
      acquired = !unlocked;
    }
  } finally {
    // If explicit unlock failed, closing this dedicated session is the
    // authoritative stale-claim reclamation path.
    client.release(acquired ? new Error('HEAVY_WORK_ADMISSION_RELEASE') : undefined);
  }
}
