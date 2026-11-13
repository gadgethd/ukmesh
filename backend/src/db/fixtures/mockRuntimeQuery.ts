import type { TestContext } from 'node:test';
import pg from 'pg';
import { pool, analyticsPool } from '../runtime.js';

/** Test seam for both legacy pool.query and explicitly checked-out reads.
 * Only the process-default runtimes are redirected; fixture-owned pools keep
 * their real connections and transaction behaviour.
 */
export function mockRuntimeQuery(t: TestContext, query: (sql: string, params: unknown[]) => unknown) {
  const legacy = t.mock.method(pg.Pool.prototype, 'query', query);
  const oltp = t.mock.method(pool, 'connect', async () => ({ query, release() {} }));
  const analytics = t.mock.method(analyticsPool, 'connect', async () => ({ query, release() {} }));
  return { mock: { restore() { analytics.mock.restore(); oltp.mock.restore(); legacy.mock.restore(); } } };
}
