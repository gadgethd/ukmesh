import type { TestContext } from 'node:test';
import { pool, analyticsPool } from '../runtime.js';

/** Test seam for both legacy pool.query and explicitly checked-out reads.
 * Only the process-default runtimes are redirected; fixture-owned pools keep
 * their real connections and transaction behaviour.
 */
export function mockRuntimeQuery(t: TestContext, query: (sql: string, params: unknown[]) => unknown) {
  const mocks = [pool, analyticsPool].flatMap(target => [
    t.mock.method(target, 'query', query),
    t.mock.method(target, 'connect', async () => ({ query, release() {} })),
  ]);
  return { mock: { restore() { for (const mocked of [...mocks].reverse()) mocked.mock.restore(); } } };
}
