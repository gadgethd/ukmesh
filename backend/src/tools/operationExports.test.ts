import assert from 'node:assert/strict';
import test from 'node:test';
import { createExportPacer, exportIdentityChecks } from './operationExports.js';
import policy from './operationPairPolicy.json' with { type: 'json' };

test('an early timer cannot shorten the actual actor cooldown or delay a different actor', async () => {
  let now = 0, early = true;
  const waits: number[] = [];
  const pacer = createExportPacer(['first', 'second'], () => now, async ms => {
    waits.push(ms); now += early ? ms - 1 : ms; early = false;
  });
  pacer.complete('first');
  await pacer.wait('second');
  assert.deepEqual(waits, []);
  await pacer.wait('first');
  assert.equal(now, 13000);
  assert.deepEqual(waits, [13000, 1]);
  pacer.complete('first');
  now += 13000;
  await pacer.wait('first');
  assert.equal(waits.length, 2, 'the exact cooldown boundary is eligible');
  await assert.rejects(pacer.wait('foreign'), /unknown.*actor/);
});

test('eight serial clients share CSV and GeoJSON cooldowns across three trials within their real rate limit', async () => {
  const actors = Array.from({ length: 8 }, (_, index) => `127.0.6.${index + 1}`);
  let now = 0, request = 0;
  const pacer = createExportPacer(actors, () => now, async ms => { now += ms; });
  const exports: Array<{ actor: string; started: number; finished: number; name: string; trial: number }> = [];
  for (let trial = 0; trial < 3; trial++) {
    for (const name of ['summary', 'charts', 'nodes-csv', 'nodes-geojson']) {
      for (let sample = 0; sample < 20; sample++) {
        const actor = actors[request++ % 8];
        const exporting = name.startsWith('nodes-');
        if (exporting) await pacer.wait(actor);
        const started = now;
        now += 100;
        if (exporting) { exports.push({ actor, started, finished: now, name, trial }); pacer.complete(actor); }
      }
    }
  }
  assert.equal(request, 240); assert.equal(exports.length, 120);
  assert.ok(exports.some((row, index) => index > 0 && row.started - exports[index - 1].finished < 13000),
    'independent actors must not inherit a global cooldown');
  for (const actor of actors) {
    const rows = exports.filter(row => row.actor === actor);
    assert.equal(rows.length, 15);
    for (let index = 1; index < rows.length; index++) assert.ok(rows[index].started - rows[index - 1].finished >= 13000);
    for (const row of rows) assert.ok(rows.filter(other => other.started >= row.started && other.started < row.started + 60000).length <= 5);
  }
});

test('every export identity and both complete twenty-sample formats are required in every trial', () => {
  const records = Array.from({ length: 3 }, (_, trial) => ['nodes-csv', 'nodes-geojson'].flatMap(name =>
    Array.from({ length: 20 }, (_, sample) => ({ trial, name, sample, identityDigest: 'a'.repeat(64) })))).flat();
  assert.equal(policy.exportSamples, 20);
  assert.ok(exportIdentityChecks(records).every(check => check.passed));
  for (const fault of ['last-csv', 'last-geojson', 'missing', 'wrong-format', 'invalid']) {
    const changed = structuredClone(records);
    if (fault === 'missing') changed.pop();
    else if (fault === 'wrong-format') changed[changed.length - 1].name = 'nodes-csv';
    else changed[fault === 'last-csv' ? 19 : changed.length - 1].identityDigest = fault === 'invalid' ? 'invalid' : 'b'.repeat(64);
    assert.ok(exportIdentityChecks(changed).some(check => !check.passed), fault);
  }
});
