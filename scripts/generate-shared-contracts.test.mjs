import assert from 'node:assert/strict';
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { generate } from './generate-shared-contracts.mjs';

const publicData = JSON.parse(fs.readFileSync(new URL('../contracts/public-data.json', import.meta.url)));
const link = JSON.parse(fs.readFileSync(new URL('../contracts/link-v3.json', import.meta.url)));
const lua = fs.readFileSync(new URL('../contracts/link-v3-admit.lua', import.meta.url), 'utf8');

test('public bindings preserve channel identities without exposing crypto material', () => {
  const outputs = generate(publicData, link, lua);
  const frontend = outputs.get('frontend/src/contracts/publicData.generated.ts');
  assert.equal(publicData.channels.length, 41);
  assert.deepEqual(publicData.channels[0], { label: 'Public', slug: 'public' });
  assert.ok(publicData.channels.some(c => c.slug === 'public2'));
  assert.ok(publicData.channels.some(c => c.slug === 'yorks'));
  assert.ok(publicData.channels.some(c => c.slug === 'yorkshire'));
  assert.doesNotMatch(frontend, /secret|keyStore|MESHCORE_CHANNEL_SECRETS|[a-f0-9]{32}/);
  const confidential = structuredClone(publicData);
  confidential.channels[0].secret = 'do-not-publish';
  assert.throws(() => generate(confidential, link, lua), /Invalid public channel/);
});

test('invalid protocol bindings fail generation before adapters can drift', () => {
  const unknownKey = structuredClone(link);
  unknownKey.admitKeys[0] = 'unknown';
  assert.throws(() => generate(publicData, unknownKey, lua), /Invalid link-v3 bindings/);
  const duplicateKey = structuredClone(link);
  duplicateKey.admitKeys[0] = duplicateKey.admitKeys[1];
  assert.throws(() => generate(publicData, duplicateKey, lua), /Invalid link-v3 bindings/);
  const invalidLimit = structuredClone(link);
  invalidLimit.limits.maxJobs.default = invalidLimit.limits.maxJobs.max + 1;
  assert.throws(() => generate(publicData, invalidLimit, lua), /Invalid link-v3 limits/);
});

test('TS and Python adapters expose identical admission order and scripts', () => {
  const outputs = generate(publicData, link, lua);
  const result = spawnSync('python3', ['-c', [
    'import json, sys',
    'scope = {}',
    'exec(sys.stdin.read(), scope)',
    "print(json.dumps({'keys': scope['ADMIT_KEYS'], 'lua': scope['ADMIT_SCRIPT'], 'limits': scope['LINK_V3_LIMITS']}))",
  ].join('\n')], { input: outputs.get('viewshed-worker/link_protocol_generated.py'), encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  const python = JSON.parse(result.stdout);
  assert.deepEqual(python.keys, [
    'meshcore:link:v3:ready', 'meshcore:link:v3:payloads', 'meshcore:link:v3:states',
    'meshcore:link:v3:attempts', 'meshcore:link:v3:bytes', 'meshcore:link:v3:dedupe',
    'meshcore:link:v3:dedupe_by_job', 'meshcore:link:v3:deferred', 'meshcore:link:v3:rebuild',
    'meshcore:link:v3:counters', 'meshcore:link:v3:events', 'meshcore:link:v3:enqueued',
    'meshcore:link:v3:wake',
  ]);
  assert.equal(python.lua, lua);
  assert.deepEqual(python.limits, link.limits);
  assert.deepEqual([...generate(publicData, link, lua)], [...outputs]);
});
