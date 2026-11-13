import { randomUUID } from 'node:crypto';
import type { TestContext } from 'node:test';
import pg from 'pg';

type Cleanup = () => void | Promise<void>;
const testDisposals = new WeakMap<object, Array<() => Promise<void>>>();

function registerDisposal(t: Pick<TestContext, 'after'>, dispose: () => Promise<void>) {
  let disposals = testDisposals.get(t);
  if (!disposals) {
    disposals = [];
    testDisposals.set(t, disposals);
    t.after(async () => {
      const results = await Promise.allSettled(disposals!.map(close => close()));
      testDisposals.delete(t);
      const errors = results.filter(result => result.status === 'rejected').map(result => result.reason);
      if (errors.length) throw new AggregateError(errors, 'Disposable database cleanup failed');
    });
  }
  disposals.push(dispose);
}

/** Own only a unique loopback database; callers retain their real pools/schema. */
export async function createDisposableDatabase(
  t: Pick<TestContext, 'after'>,
  endpoint: string,
  prefix: string,
) {
  const url = new URL(endpoint);
  if (!['postgres:', 'postgresql:'].includes(url.protocol)
      || !['localhost', '127.0.0.1', '[::1]', '::1'].includes(url.hostname)
      || url.searchParams.has('host') || url.searchParams.has('hostaddr')
      || !url.pathname || url.pathname === '/') {
    throw new Error('Disposable databases require a loopback PostgreSQL endpoint');
  }
  if (!/^[a-z][a-z0-9_]{0,35}$/.test(prefix)) throw new Error('Invalid disposable database prefix');
  const name = `${prefix}_${process.pid}_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
  const admin = new pg.Client({ connectionString: endpoint });
  try {
    await admin.connect();
    await admin.query(`CREATE DATABASE "${name}"`);
  } catch (error) {
    await admin.end();
    throw error;
  }
  const cleanups: Cleanup[] = [];
  let closing: Promise<void> | undefined;
  const dispose = () => closing ??= Promise.resolve().then(async () => {
    const errors: unknown[] = [];
    for (const cleanup of cleanups.reverse()) {
      try { await cleanup(); } catch (error) { errors.push(error); }
    }
    try {
      // Pool.end() may resolve before socket close. PostgreSQL waits for those
      // disconnects on a normal DROP instead of terminating closing clients.
      await admin.query(`DROP DATABASE "${name}"`);
    } catch (error) {
      errors.push(error);
    } finally {
      try { await admin.end(); } catch (error) { errors.push(error); }
    }
    if (errors.length) throw new AggregateError(errors, `Failed to dispose database ${name}`);
  });
  registerDisposal(t, dispose);
  url.pathname = `/${name}`;
  return {
    name,
    url: url.href,
    addCleanup(cleanup: Cleanup) {
      if (closing) throw new Error('Disposable database is already closing');
      cleanups.push(cleanup);
    },
    dispose,
  };
}
