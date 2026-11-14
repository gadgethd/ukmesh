import { constants } from 'node:fs';
import { open, realpath, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import type { LookupSourceIdentity } from './lookupSource.js';
import type { OperationSnapshot } from './operationResponse.js';
import policy from './operationPairPolicy.json' with { type: 'json' };

export const OPERATION_PAIR_POLICY = Object.freeze(policy);
const NAMES = new Set(['summary', 'charts', 'nodes-csv', 'nodes-geojson']);

async function document(file: string): Promise<any> {
  const descriptor = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stat = await descriptor.stat();
    if (!stat.isFile() || stat.size > 32_768) throw new Error('invalid operation pair document');
    return JSON.parse(await descriptor.readFile('utf8'));
  } finally { await descriptor.close(); }
}

/** One controller owns releases; each source process owns only its readiness. */
export async function createOperationPair(directory: string, token: string, label: string,
  source: LookupSourceIdentity, timeoutMs = policy.barrierTimeoutMs) {
  if (!/^[a-f0-9]{32}$/.test(token) || !['baseline', 'candidate'].includes(label)
    || !Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > policy.barrierTimeoutMs) throw new Error('invalid operation pair identity');
  const root = await realpath(directory);
  if (root !== path.resolve(directory)) throw new Error('operation pair directory must be canonical');
  const manifest = await document(path.join(root, 'pair.json'));
  if (manifest?.token !== token || !isDeepStrictEqual(manifest.sources?.[label], source)
    || typeof manifest.fixture_token !== 'string') throw new Error('operation pair source or fixture differs');
  return {
    token, label,
    async wait(trial: number, name: string, snapshot: OperationSnapshot) {
      if (!Number.isInteger(trial) || trial < 0 || trial > 2 || !NAMES.has(name)) throw new Error('invalid operation pair step');
      const step = `${trial}-${name}`;
      const deadline = performance.now() + timeoutMs;
      const withinDeadline = () => {
        if (performance.now() >= deadline) throw new Error('operation pair readiness deadline exceeded');
      };
      const readyAt = new Date().toISOString();
      if (!Number.isFinite(Date.parse(snapshot.generatedAt)) || !Number.isFinite(snapshot.maxAgeMs)
        || Date.now() - Date.parse(snapshot.generatedAt) > snapshot.maxAgeMs) throw new Error('operation pair chart snapshot expired');
      if (trial === 0 && name === 'summary'
        && Date.parse(snapshot.generatedAt) + snapshot.maxAgeMs - Date.now() < policy.minSnapshotFreshMs) {
        throw new Error('operation pair requires five minutes of chart freshness');
      }
      await writeFile(path.join(root, `${step}-${label}.ready.json`),
        JSON.stringify({ token, label, trial, name, source, snapshot, readyAt }), { flag: 'wx', mode: 0o600 });
      withinDeadline();
      while (performance.now() < deadline) {
        try {
          const abort = await document(path.join(root, 'abort.json'));
          withinDeadline();
          if (abort?.token === token) throw new Error('operation pair controller aborted');
        } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
        try {
          const release = await document(path.join(root, `${step}.release.json`));
          withinDeadline();
          const at = Date.parse(release?.releaseAt);
          if (release?.token !== token || release.trial !== trial || release.name !== name
            || !Number.isFinite(at) || at < Date.parse(readyAt) || at < Date.now() - 5000 || at > Date.now() + 1000) {
            throw new Error('invalid operation pair release');
          }
          if (Date.now() - Date.parse(snapshot.generatedAt) > snapshot.maxAgeMs) throw new Error('operation pair chart snapshot expired');
          await new Promise(resolve => setTimeout(resolve, Math.max(0, at - Date.now())));
          withinDeadline();
          return { token, readyAt, releaseAt: release.releaseAt };
        } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      throw new Error('operation pair readiness deadline exceeded');
    },
  };
}
