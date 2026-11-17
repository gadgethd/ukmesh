import assert from 'node:assert/strict';
import { access, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { execFileSync } from 'node:child_process';
import { createOperationPair } from './operationPair.js';

const token = 'a'.repeat(32);
const sources = Object.fromEntries(['baseline', 'candidate'].map(label => [label,
  { files: 1, revision: 'b'.repeat(40), sha256: 'c'.repeat(64), source_root: '/' + label }]));
const snapshot = () => ({ generatedAt: new Date().toISOString(), visibilityGeneration: 7, maxAgeMs: 1800000 });
async function fixture(t: { after: (work: () => Promise<void>) => void }) {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'operation-pair-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await writeFile(path.join(directory, 'pair.json'), JSON.stringify({ token, fixture_token: 'owned', sources }));
  return directory;
}
async function ready(file: string) {
  for (let step = 0; step < 100; step++) {
    try { return JSON.parse(await readFile(file, 'utf8')); }
    catch { await new Promise(resolve => setTimeout(resolve, 2)); }
  }
  throw new Error('readiness was not written');
}

test('separate source owners wait for the same controller release before work', async t => {
  const directory = await fixture(t), state = snapshot();
  const owners = await Promise.all(['baseline', 'candidate'].map(label => createOperationPair(directory, token, label, sources[label])));
  const pending = owners.map(owner => owner.wait(0, 'summary', state));
  const records = await Promise.all(['baseline', 'candidate'].map(label => ready(path.join(directory, `0-summary-0-${label}.ready.json`))));
  assert.deepEqual(records.map(value => value.source), [sources.baseline, sources.candidate]);
  const releaseAt = new Date(Date.now() + 30).toISOString();
  await writeFile(path.join(directory, '0-summary-0.release.json'), JSON.stringify({ token, trial: 0, name: 'summary', sample: 0, releaseAt }));
  const result = await Promise.all(pending);
  assert.ok(Date.now() >= Date.parse(releaseAt));
  assert.deepEqual(result.map(value => value.releaseAt), [releaseAt, releaseAt]);
  await assert.rejects(owners[0].wait(0, 'summary', state), /EEXIST/);
});

test('silent, aborted and malformed controller releases fail within a bounded wait', async t => {
  for (const fault of ['silent', 'late', 'abort', 'wrong-token', 'wrong-step', 'symlink', 'fifo']) {
    const directory = await fixture(t);
    const owner = await createOperationPair(directory, token, 'baseline', sources.baseline, ['silent', 'late'].includes(fault) ? 25 : 1000);
    const waiting = owner.wait(0, 'summary', snapshot());
    const rejection = assert.rejects(waiting, /deadline|aborted|invalid operation pair release|ELOOP|invalid operation pair document/);
    await ready(path.join(directory, '0-summary-0-baseline.ready.json'));
    if (fault === 'abort') await writeFile(path.join(directory, 'abort.json'), JSON.stringify({ token }));
    else if (fault === 'fifo') execFileSync('mkfifo', [path.join(directory, '0-summary-0.release.json')]);
    else if (fault !== 'silent') {
      const release = { token: fault === 'wrong-token' ? 'd'.repeat(32) : token,
        trial: fault === 'wrong-step' ? 1 : 0, name: 'summary', sample: 0, releaseAt: new Date(Date.now() + (fault === 'late' ? 100 : 0)).toISOString() };
      const destination = path.join(directory, fault === 'symlink' ? 'foreign.json' : '0-summary-0.release.json');
      await writeFile(destination, JSON.stringify(release));
      if (fault === 'symlink') await symlink(destination, path.join(directory, '0-summary-0.release.json'));
    }
    await rejection;
  }
});

test('wrong source, insufficient chart lifetime and invalid steps cannot join the pair', async t => {
  const directory = await fixture(t);
  await assert.rejects(createOperationPair(directory, token, 'baseline', sources.candidate), /source or fixture/);
  const owner = await createOperationPair(directory, token, 'baseline', sources.baseline);
  await assert.rejects(owner.wait(0, 'summary', { ...snapshot(), maxAgeMs: 299000 }), /five minutes/);
  await assert.rejects(owner.wait(0, '../outside', snapshot()), /invalid operation pair step/);
  await assert.rejects(owner.wait(3, 'summary', snapshot()), /invalid operation pair step/);
  await assert.rejects(access(path.join(directory, '0-summary-0-baseline.ready.json')));
});

test('an early scheduler wake cannot release work before its actual wall-clock boundary', async t => {
  const directory = await fixture(t);
  const owner = await createOperationPair(directory, token, 'baseline', sources.baseline, 1000);
  const schedule = globalThis.setTimeout;
  let early = false;
  t.mock.method(globalThis, 'setTimeout', (callback, delay, ...args) => {
    if (!early && delay > 100) { early = true; return schedule(callback, 0, ...args); }
    return schedule(callback, delay, ...args);
  });
  const waiting = owner.wait(0, 'summary', snapshot());
  await ready(path.join(directory, '0-summary-0-baseline.ready.json'));
  const releaseAt = new Date(Date.now() + 500).toISOString();
  await writeFile(path.join(directory, '0-summary-0.release.json'), JSON.stringify({ token, trial: 0, name: 'summary', sample: 0, releaseAt }));
  await waiting;
  assert.equal(early, true, 'test must actually deliver an early timer callback');
  assert.ok(Date.now() >= Date.parse(releaseAt), 'actual work must still wait for the unchanged release boundary');
});
