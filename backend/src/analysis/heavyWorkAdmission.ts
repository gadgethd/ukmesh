import type { Pool, PoolClient, QueryResultRow } from 'pg';

// One database-scoped session advisory lock serialises analytical and fixture
// preparation work across processes. PostgreSQL owns the claim, so a
// crashed process or severed connection releases it without a stale lock row.
const HEAVY_WORK_LOCK_CLASS = 1_433_214_840;
const HEAVY_WORK_LOCK_KEY = 1_463_235_684;
const DEFAULT_RETRY_MS = 1_000;

type AdmissionClient = Pick<PoolClient, 'query' | 'release'>;
type AdmissionPool = Pick<Pool, 'connect'>;

export type HeavyWorkAdmissionOptions<T> = {
  pool: AdmissionPool;
  workload: string;
  task: (session: Pick<PoolClient, 'query'>) => Promise<T>;
  signal?: AbortSignal;
  retryMs?: number;
  log?: Pick<Console, 'log' | 'warn'>;
};

export type HeavyWorkAdmissionResult<T> =
  | { acquired: false }
  | { acquired: true; value: T };

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
  if (result.rows.length !== 1 || typeof result.rows[0]?.acquired !== 'boolean') {
    throw new Error('INVALID_HEAVY_WORK_ADMISSION_RESULT');
  }
  return result.rows[0].acquired;
}

export async function withHeavyWorkAdmission<T>(
  options: HeavyWorkAdmissionOptions<T>,
): Promise<T> {
  const result = await runAdmission(options, true);
  if (!result.acquired) throw new Error('HEAVY_WORK_ADMISSION_NOT_ACQUIRED');
  return result.value;
}

export function tryWithHeavyWorkAdmission<T>(
  options: Omit<HeavyWorkAdmissionOptions<T>, 'retryMs'>,
): Promise<HeavyWorkAdmissionResult<T>> {
  return runAdmission(options, false);
}

async function runAdmission<T>(
  options: HeavyWorkAdmissionOptions<T>,
  wait: boolean,
): Promise<HeavyWorkAdmissionResult<T>> {
  if (!/^[a-z0-9][a-z0-9:_-]{0,95}$/i.test(options.workload)) {
    throw new Error('INVALID_HEAVY_WORKLOAD_NAME');
  }
  const retryMs = Math.max(1, Math.min(30_000, Math.trunc(options.retryMs ?? DEFAULT_RETRY_MS)));
  const logger = options.log ?? console;
  const client = await options.pool.connect() as AdmissionClient;
  const waitingStartedAt = Date.now();
  let acquired = false;
  let waitingLogged = false;
  let discard = false;
  try {
    while (!acquired) {
      options.signal?.throwIfAborted();
      acquired = await tryAcquire(client);
      if (acquired) break;
      options.signal?.throwIfAborted();
      if (!wait) return { acquired: false };
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
    let taskFailure: { value: unknown } | undefined;
    try {
      options.signal?.throwIfAborted();
      return { acquired: true, value: await options.task(client) };
    } catch (error) {
      taskFailure = { value: error };
      discard = true;
      throw error;
    } finally {
      let unlockFailure: { value: unknown } | undefined;
      let unlocked = false;
      try {
        const unlock = await client.query<QueryResultRow>(
          'SELECT pg_advisory_unlock($1, $2) AS unlocked',
          [HEAVY_WORK_LOCK_CLASS, HEAVY_WORK_LOCK_KEY],
        );
        unlocked = unlock.rows[0]?.['unlocked'] === true;
      } catch (error) {
        unlockFailure = { value: error };
        logger.warn(
          `[db-admission] workload=${options.workload} unlock failed: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
      logger.log(
        `[db-admission] workload=${options.workload} released durationMs=${Date.now() - acquiredAt} unlocked=${unlocked}`,
      );
      acquired = !unlocked;
      if (!unlocked) {
        discard = true;
        const error = unlockFailure ? unlockFailure.value : new Error('HEAVY_WORK_ADMISSION_UNLOCK_FAILED');
        if (taskFailure) {
          throw new AggregateError([taskFailure.value, error], 'HEAVY_WORK_ADMISSION_TASK_AND_UNLOCK_FAILED');
        }
        throw error;
      }
    }
  } catch (error) {
    discard = true;
    throw error;
  } finally {
    // A failed task may retain transaction or nested session-lock state even
    // after our explicit unlock. This owner alone reclaims that session.
    client.release(acquired || discard ? new Error('HEAVY_WORK_ADMISSION_RELEASE') : undefined);
  }
}
