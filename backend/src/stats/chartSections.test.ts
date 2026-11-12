import assert from 'node:assert/strict';
import test from 'node:test';
import express from 'express';
import { once } from 'node:events';
import { parseChartSections, projectChartSections, CHART_SECTIONS } from './chartSections.js';
import { publicJson } from '../http/publicJson.js';

test('section allowlist rejects ambiguous/unbounded query shapes and canonicalizes order', () => {
  assert.equal(parseChartSections(undefined), undefined);
  assert.deepEqual(parseChartSections('signal,overview'), ['overview', 'signal']);
  for (const bad of ['', 'overview,overview', 'all', 'Overview', ' overview', ['overview'], {}, 'paths,'.repeat(20)]) {
    assert.throws(() => parseChartSections(bad));
  }
});

test('projection preserves full compatibility and never mutates the cached snapshot', () => {
  const full: Record<string, unknown> = { snapshot: { visibilityGeneration: 4 }, summary: { totalPackets24h: 12 }, topChatters: [] };
  for (const fields of Object.values(CHART_SECTIONS)) for (const field of fields) full[field] = Object.freeze([]);
  Object.freeze(full);
  assert.equal(projectChartSections(full, undefined, { network: 'test', observer: null }, 4), full);
  const selected = projectChartSections(full, ['signal'], { network: 'test', observer: 'F'.repeat(64) }, 4) as Record<string, unknown>;
  assert.deepEqual(Object.keys(selected).sort(), ['representation', 'signalSummary', 'snapshot', 'summary']);
  assert.equal(selected.signalSummary, full.signalSummary);
  assert.throws(() => projectChartSections(full, ['signal'], { network: 'test', observer: null }, 5));
  assert.throws(() => projectChartSections({ snapshot: { visibilityGeneration: 4 } }, ['paths'], { network: 'test', observer: null }, 4));
});

test('conditional section responses isolate representation/scope and fail closed during a policy change', async t => {
  let generation = 1;
  let changeDuringLoad = false;
  let unavailable = false;
  const app = express();
  app.get('/', async (req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    let sections;
    try { sections = parseChartSections(req.query.sections); }
    catch { res.status(400).json({ error: 'Invalid sections' }); return; }
    try {
      await publicJson(res, async () => { if (unavailable) throw new Error('no authority'); return generation; }, async before => {
        const full: Record<string, unknown> = { snapshot: { visibilityGeneration: before }, summary: {} };
        for (const fields of Object.values(CHART_SECTIONS)) for (const field of fields) full[field] = [];
        if (changeDuringLoad) generation++;
        return projectChartSections(full, sections, { network: String(req.query.network ?? 'ukmesh'), observer: String(req.query.observer ?? '') || null }, before);
      });
    } catch { res.status(503).json({ error: 'Unavailable' }); }
  });
  const server = app.listen(0, '127.0.0.1');
  t.after(() => { server.closeAllConnections(); server.close(); });
  await once(server, 'listening');
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const base = `http://127.0.0.1:${address.port}/?sections=overview`;
  const first = await fetch(base);
  const etag = first.headers.get('etag')!;
  const conditional = { cache: 'force-cache' as const, headers: { 'If-None-Match': etag } };
  assert.equal((await fetch(base, conditional)).status, 304);
  for (const query of ['&network=test', '&observer=synthetic', '&sections=signal']) {
    const url = query.includes('sections') ? base.replace('overview', 'signal') : base + query;
    assert.equal((await fetch(url, conditional)).status, 200);
  }
  assert.equal((await fetch(base + ',overview')).status, 400);
  for (const fail of ['change', 'unavailable']) {
    changeDuringLoad = fail === 'change'; unavailable = fail === 'unavailable';
    const result = await fetch(base, conditional);
    assert.equal(result.status, 503);
    assert.equal(result.headers.get('cache-control'), 'no-store');
  }
});
