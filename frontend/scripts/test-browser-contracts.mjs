import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { mkdir, writeFile } from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = fileURLToPath(new URL('../', import.meta.url));
const output = path.join(root, 'test-results/browser-contracts');
const site = path.join(output, 'site');
await mkdir(output, { recursive: true });
const cancelled = new AbortController();
const interrupt = () => cancelled.abort(new Error('Browser contract run interrupted'));
process.once('SIGINT', interrupt);
process.once('SIGTERM', interrupt);

function terminate(child, signal = 'SIGTERM') {
  if (!child.pid) return;
  if (child.exitCode !== null || child.signalCode !== null) return;
  try {
    if (process.platform === 'win32') child.kill(signal);
    else process.kill(-child.pid, signal);
  } catch (error) { if (error.code !== 'ESRCH') throw error; }
}

function start(command, args, env, logName) {
  const log = createWriteStream(path.join(output, logName));
  const child = spawn(command, args, { cwd: root, env, detached: process.platform !== 'win32', stdio: ['ignore', 'pipe', 'pipe'] });
  let text = '';
  const capture = chunk => {
    log.write(chunk);
    process.stdout.write(chunk);
    text = (text + String(chunk)).slice(-1_048_576);
  };
  child.stdout.on('data', capture);
  child.stderr.on('data', capture);
  const abort = () => terminate(child);
  cancelled.signal.addEventListener('abort', abort, { once: true });
  const done = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => {
      cancelled.signal.removeEventListener('abort', abort);
      log.end();
      if (code === 0) resolve(text);
      else reject(new Error(`${command} ${args[0]} exited with ${code ?? signal}; see ${path.join(output, logName)}`));
    });
  });
  // Preview startup may fail while readiness is being checked; retain the error
  // for the awaited result without producing an unhandled rejection.
  void done.catch(() => {});
  if (cancelled.signal.aborted) abort();
  return { child, done };
}

async function freePort() {
  const listener = createServer();
  await new Promise((resolve, reject) => { listener.once('error', reject); listener.listen(0, '127.0.0.1', resolve); });
  const port = listener.address().port;
  await new Promise((resolve, reject) => listener.close(error => error ? reject(error) : resolve()));
  return port;
}

let preview;
let report = { buildTarget: 'public-website', passed: false, startedAt: new Date().toISOString() };
await writeFile(path.join(output, 'summary.json'), `${JSON.stringify(report, null, 2)}\n`);
try {
  const env = { ...process.env, VITE_BUILD_TARGET: 'public-website', VITE_APP_HOSTNAME: 'app.invalid', VITE_SITE: 'ukmesh' };
  await start('npm', ['run', 'build', '--', '--config', 'vite.e2e.config.ts', '--outDir', site], env, 'build.log').done;
  const port = await freePort();
  const origin = `http://127.0.0.1:${port}`;
  report.origin = origin;
  preview = start(process.execPath, [path.join(root, 'node_modules/vite/bin/vite.js'), 'preview', '--config', 'vite.e2e.config.ts',
    '--host', '127.0.0.1', '--port', String(port), '--strictPort', '--outDir', site], env, 'preview.log');
  const deadline = Date.now() + 30_000;
  for (;;) {
    cancelled.signal.throwIfAborted();
    if (preview.child.exitCode !== null || preview.child.signalCode !== null) await preview.done;
    try { if ((await fetch(origin, { signal: AbortSignal.timeout(500) })).ok) break; } catch {}
    if (Date.now() >= deadline) throw new Error('Owned browser preview did not become ready within 30 seconds');
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  const text = await start(process.execPath, ['--import', 'tsx', '--test', '--test-reporter=tap', '--test-concurrency=1',
    'src/pages/OwnerPortalPage.browser.test.ts', 'src/pages/TopologyPage.browser.test.ts', 'src/pages/RegionsPage.browser.test.ts'],
  { ...env, UKMESH_BROWSER_TESTS: '1', UKMESH_BROWSER_BASE_URL: origin, NO_COLOR: '1' }, 'tests.log').done;
  const tests = Number(text.match(/^# tests (\d+)$/m)?.[1]);
  const skipped = Number(text.match(/^# skipped (\d+)$/m)?.[1]);
  if (!(tests > 0) || skipped !== 0) throw new Error('Browser contract lane must execute tests with zero skips');
  report = { ...report, passed: true, tests, skipped, origin };
} catch (error) {
  report.error = error.message;
  console.error(error.message);
  process.exitCode = cancelled.signal.aborted ? 130 : 1;
} finally {
  if (preview) {
    terminate(preview.child);
    const force = setTimeout(() => terminate(preview.child, 'SIGKILL'), 3_000);
    try { await preview.done; } catch {} finally { clearTimeout(force); }
  }
  await writeFile(path.join(output, 'summary.json'), `${JSON.stringify({ ...report, completedAt: new Date().toISOString() }, null, 2)}\n`);
  process.removeListener('SIGINT', interrupt);
  process.removeListener('SIGTERM', interrupt);
}
