import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import test from 'node:test';
import { runRegionSnapshotChild } from './snapshotChild.js';

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
