import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

/** Deadline/abort applies to SQL, decode and publication, including a stuck child. */
export function runRegionSnapshotChild(signal: AbortSignal, options: {
  timeoutMs: number;
  script?: URL;
  env?: NodeJS.ProcessEnv;
  killGraceMs?: number;
}): Promise<void> {
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--max-old-space-size=640',
      fileURLToPath(options.script ?? new URL('../tools/rebuildRegionParticipation.js', import.meta.url))], {
      env: { ...process.env, ...options.env, REGION_SCOPE_ALLOW_EMPTY: '1' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let output = '', timedOut = false, ended = false;
    let hardKill: NodeJS.Timeout | undefined;
    const capture = (data: Buffer) => { output = (output + data.toString('utf8')).slice(-8_192); };
    child.stdout.on('data', capture); child.stderr.on('data', capture);
    const stop = () => {
      if (ended) return;
      child.kill('SIGTERM');
      if (!hardKill) hardKill = setTimeout(() => { if (!ended) child.kill('SIGKILL'); }, options.killGraceMs ?? 2_000);
    };
    const deadline = setTimeout(() => { timedOut = true; stop(); }, options.timeoutMs);
    const cleanup = () => {
      ended = true; clearTimeout(deadline); if (hardKill) clearTimeout(hardKill);
      signal.removeEventListener('abort', stop);
    };
    signal.addEventListener('abort', stop, { once: true });
    if (signal.aborted) stop();
    child.once('error', error => { cleanup(); reject(error); });
    child.once('close', (code, exitSignal) => {
      cleanup();
      if (signal.aborted) { reject(signal.reason ?? new Error('region snapshot aborted')); return; }
      if (timedOut) { reject(new Error('REGION_SNAPSHOT_DEADLINE')); return; }
      if (code !== 0) { reject(new Error(`REGION_SNAPSHOT_CHILD_FAILED (${code ?? exitSignal}): ${output}`)); return; }
      resolve();
    });
  });
}
