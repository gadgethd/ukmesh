// One bounded test process owns the eight real HTTP client identities.
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {randomUUID} from 'node:crypto';
import {fileURLToPath} from 'node:url';

const execute = promisify(execFile);
const quote = value => "'" + value.replaceAll("'", "'\\''") + "'";
async function docker(args) {
  return execute('sg', ['docker', '-c', ['docker', '--host', 'unix:///var/run/docker.sock', ...args].map(quote).join(' ')],
    {timeout: 20000, maxBuffer: 1048576, env: {PATH: process.env.PATH, HOME: process.env.HOME, LANG: 'C.UTF-8'}});
}

export async function startInterfaceReaders() {
  const backend = JSON.parse((await docker(['inspect', 'ukmesh-full-backend-1'])).stdout)[0];
  assert.equal(backend.Config.Labels['com.docker.compose.project'], 'ukmesh-full');
  assert.equal(backend.State.Running, true);
  const identity = randomUUID().replaceAll('-', '').slice(0, 12);
  const name = 'ukmesh-interface-' + identity;
  async function finish() {
    let owned = false;
    try {
      let actual = JSON.parse((await docker(['inspect', name])).stdout)[0];
      assert.equal(actual.Config.Labels['ukmesh.interface-proof'], identity); owned = true;
      assert.equal(actual.State.Running, true, 'the reader workload must cover the complete interface flow');
      await docker(['stop', '--timeout', '15', name]);
      actual = JSON.parse((await docker(['inspect', name])).stdout)[0];
      const lines = (await docker(['logs', name])).stdout.trim().split('\n');
      const value = JSON.parse(lines.at(-1));
      return {...value, activeThroughFinish: true, passed: value.passed && actual.State.ExitCode === 0};
    } finally {if (owned) await docker(['rm', '-f', name]);}
  }
  try {
    await docker(['run', '-d', '--name', name, '--label', 'ukmesh.interface-proof=' + identity,
      '--network', 'container:' + backend.Id, '--memory', '128m', '--memory-swap', '128m', '--cpus', '0.5', '--pids-limit', '32',
      '--read-only', '--tmpfs', '/tmp:rw,size=2m', '--entrypoint', 'node',
      '-v', fileURLToPath(new URL('./interface-read-client.mjs', import.meta.url)) + ':/proof.mjs:ro',
      backend.Image, '--max-old-space-size=32', '/proof.mjs']);
    const deadline = Date.now() + 15000;
    let ready = false;
    while (Date.now() < deadline && !ready) {
      const lines = (await docker(['logs', name])).stdout.trim().split('\n').filter(Boolean);
      const messages = lines.map(line => JSON.parse(line));
      const failed = messages.find(message => message.passed === false);
      if (failed) throw new Error('Read workload preflight failed: ' + (failed.error ?? 'no native readiness proof'));
      ready = messages.some(message => message.ready === true);
      if (!ready) await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(ready, true, 'all eight readers must be ready before interface operations');
    return {finish};
  } catch (error) {await finish().catch(() => {}); throw error;}
}
