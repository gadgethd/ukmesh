// Native public-source proofs share polling, HTTP and read-only SQLite access.
import {DatabaseSync} from 'node:sqlite';
import path from 'node:path';

export async function until(check, label, timeout = 30_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw new Error('Timed out waiting for ' + label);
}

export async function coreStatus() {
  const response = await fetch('http://127.0.0.1:3102/api/stats', {signal: AbortSignal.timeout(5000)});
  await response.body?.cancel();
  return response.status;
}

export function readProjection(directory, query, args = []) {
  const db = new DatabaseSync(path.join(directory, 'analyzer.db'), {readOnly: true});
  try { return db.prepare(query).all(...args); } finally { db.close(); }
}
