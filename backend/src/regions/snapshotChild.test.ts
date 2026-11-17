import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import test from 'node:test';
import { runRegionSnapshotChild } from './snapshotChild.js';
import { REGION_SNAPSHOT_DEFERRED_EXIT_CODE } from './snapshotOutcome.js';

test('a region child that ignores SIGTERM is killed at its deadline and cannot outlive cancellation', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'ukmesh-region-child-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const script = path.join(directory, 'child.mjs'), pidFile = path.join(directory, 'pid');
  await writeFile(script, `import fs from 'node:fs'; process.on('SIGTERM',()=>{}); fs.writeFileSync(process.env.PID_FILE,String(process.pid)); setInterval(()=>{},1000);`);
  const options = { script: pathToFileURL(script), env: { PID_FILE: pidFile }, timeoutMs: 1500, killGraceMs: 50 };
  await assert.rejects(runRegionSnapshotChild(new AbortController().signal, options), /REGION_SNAPSHOT_DEADLINE/);
  const deadlinePid = Number(await readFile(pidFile, 'utf8'));
  assert.throws(() => process.kill(deadlinePid, 0), { code: 'ESRCH' });
  await rm(pidFile);
  const shutdown = new AbortController();
  const running = runRegionSnapshotChild(shutdown.signal, { ...options, timeoutMs: 30_000 });
  const pidReadyBy = Date.now() + 5_000;
  let pid = 0;
  while (Date.now() < pidReadyBy) {
    try { pid = Number(await readFile(pidFile, 'utf8')); break; } catch { await new Promise(resolve => setTimeout(resolve, 20)); }
  }
  assert.ok(pid > 0, 'fixture child registered its handler');
  shutdown.abort(new Error('owned fixture shutdown'));
  await assert.rejects(running, /owned fixture shutdown/);
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
});

test('the child terminal protocol distinguishes completed, deferred and real failure', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'ukmesh-region-result-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const script = path.join(directory, 'child.mjs');
  const options = { script: pathToFileURL(script), timeoutMs: 5_000 };
  await writeFile(script, 'process.exitCode = 0;');
  assert.equal(await runRegionSnapshotChild(new AbortController().signal, options), 'completed');
  await writeFile(script, `process.exitCode = ${REGION_SNAPSHOT_DEFERRED_EXIT_CODE};`);
  assert.equal(await runRegionSnapshotChild(new AbortController().signal, options), 'deferred');
  await writeFile(script, 'console.error("owned child query failure"); process.exitCode = 1;');
  await assert.rejects(runRegionSnapshotChild(new AbortController().signal, options), /REGION_SNAPSHOT_CHILD_FAILED.*owned child query failure/s);
});

test('a deferred exit during deadline or cancellation cannot hide the original failure', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'ukmesh-region-deferred-stop-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const script = path.join(directory, 'child.mjs'), pidFile = path.join(directory, 'pid');
  await writeFile(script, `import fs from 'node:fs';
process.on('SIGTERM', () => process.exit(${REGION_SNAPSHOT_DEFERRED_EXIT_CODE}));
fs.writeFileSync(process.env.PID_FILE, String(process.pid)); setInterval(() => {}, 1000);`);
  const options = { script: pathToFileURL(script), env: { PID_FILE: pidFile }, timeoutMs: 1_500, killGraceMs: 50 };
  await assert.rejects(runRegionSnapshotChild(new AbortController().signal, options), /REGION_SNAPSHOT_DEADLINE/);
  async function readPid() { return Number(await readFile(pidFile, 'utf8')); }
  const deadlinePid = await readPid();
  assert.throws(() => process.kill(deadlinePid, 0), { code: 'ESRCH' });
  await rm(pidFile);
  const shutdown = new AbortController(), reason = new Error('owned deferred child cancelled');
  const running = runRegionSnapshotChild(shutdown.signal, { ...options, timeoutMs: 30_000 });
  t.after(async () => { shutdown.abort(reason); await running.catch(() => undefined); });
  let pid = 0;
  const readyBy = Date.now() + 5_000;
  while (Date.now() < readyBy) {
    try { pid = await readPid(); break; } catch { await new Promise(resolve => setTimeout(resolve, 20)); }
  }
  assert.ok(pid > 0, 'fixture child is ready before cancellation');
  shutdown.abort(reason);
  await assert.rejects(running, error => error === reason);
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
});
