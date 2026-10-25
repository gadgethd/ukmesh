import { timingSafeEqual } from 'node:crypto';
import { Router } from 'express';
import { loadCurrentPublicPrivacyPolicy } from '../db/packetBatch.js';
import { readPendingEntries, type PendingStreamEntry } from './liveStore.js';
import { encodePendingEntry, getRamIngestMode } from './protocol.js';
import type { PrivatePrefixCache } from '../privacy/privatePrefixCache.js';

function compareIds(left: string, right: string): number {
  const [lm, ls] = left.split('-').map(BigInt), [rm, rs] = right.split('-').map(BigInt);
  return lm! < rm! ? -1 : lm! > rm! ? 1 : ls! < rs! ? -1 : ls! > rs! ? 1 : 0;
}

/** Credentialed public source: consumers never receive a raw Redis principal. */
export function createPublicRamSource(deps: {
  token?: () => string | undefined;
  read?: () => Promise<PendingStreamEntry[]>;
  loadPolicy?: () => Promise<PrivatePrefixCache | null>;
} = {}): Router {
  const router = Router();
  const token = deps.token ?? (() => process.env['MQTT_RAM_PUBLIC_READ_TOKEN']);
  const read = deps.read ?? (() => readPendingEntries(10_000, true));
  const loadPolicy = deps.loadPolicy ?? loadCurrentPublicPrivacyPolicy;
  router.use((req, res, next) => {
    const expected = token();
    if (!expected || expected.length < 32) { res.sendStatus(404); return; }
    const actual = req.header('authorization') ?? '';
    const required = `Bearer ${expected}`;
    if (Buffer.byteLength(actual) !== Buffer.byteLength(required)
      || !timingSafeEqual(Buffer.from(actual), Buffer.from(required))) { res.sendStatus(401); return; }
    res.set('Cache-Control', 'no-store'); next();
  });
  router.get('/policy', async (_req, res) => {
    try {
      const policy = await loadPolicy();
      if (!policy) { res.sendStatus(503); return; }
      res.json({ generation: policy.currentGeneration });
    } catch { res.sendStatus(503); }
  });
  router.get('/entries', async (req, res) => {
    const after = String(req.query['after'] ?? '0-0');
    const limit = Number(req.query['limit'] ?? 500);
    if (!/^\d{1,20}-\d{1,20}$/.test(after) || !Number.isSafeInteger(limit) || limit < 1 || limit > 500) {
      res.sendStatus(400); return;
    }
    if (getRamIngestMode() !== 'ram') { res.sendStatus(503); return; }
    try {
      const batch = (await read()).filter(({ id }) => compareIds(id, after) > 0).slice(0, limit);
      const policy = await loadPolicy();
      if (!policy) { res.sendStatus(503); return; }
      const entries = batch.filter(({ entry }) => policy.classify({
        network: entry.input.network, rxNodeId: entry.input.rxNodeId, srcNodeId: entry.input.srcNodeId,
        pathHashes: entry.input.pathHashes, pathHashSizeBytes: entry.input.pathHashSizeBytes,
      }).visibilityOk).map(({ id, entry }) => ({ id,
        value: encodePendingEntry({ ...entry, visibilityOk: true, isPrivate: false }) }));
      res.json({ generation: policy.currentGeneration, entries, nextCursor: batch.at(-1)?.id ?? after });
    } catch { res.sendStatus(503); }
  });
  return router;
}
