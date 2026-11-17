import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import net from 'node:net';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

async function availablePort(): Promise<number> {
  const server = net.createServer();
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert.ok(address && typeof address === 'object');
  const port = address.port;
  await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  return port;
}

// Exercise the actual entrypoint and its natural process exit. The database
// boundary is in memory, so failures never depend on an external service.
for (const outcome of ['populated', 'empty', 'unavailable'] as const) {
  test(`historical link backfill closes its resources after ${outcome} database`, async () => {
    const source = `
      import { EventEmitter } from 'node:events';
      import pg from 'pg';
      const opened = new Set();
      pg.Pool = class extends EventEmitter {
        totalCount = 0; idleCount = 0; waitingCount = 0;
        constructor() { super(); opened.add(this); }
        async connect() {
          return { release() {}, async query(text) {
            if (${JSON.stringify(outcome)} === 'unavailable') throw new Error('fixture database unavailable');
            return { rows: text.includes('COUNT(*)') ? [{ count: ${outcome === 'populated' ? "'1'" : "'0'"} }] : [] };
          }};
        }
        async end() { opened.delete(this); }
      };
      process.on('beforeExit', () => console.log('remaining database pools=' + opened.size));
      await import(${JSON.stringify(new URL('./link-backfill.ts', import.meta.url).href)});
    `;
    const child = spawnSync(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', source], {
      cwd: fileURLToPath(new URL('../..', import.meta.url)),
      env: {
        PATH: process.env['PATH'],
        HOME: process.env['HOME'],
        METRICS_HOST: '127.0.0.1',
        METRICS_PORT: String(await availablePort()),
      },
      encoding: 'utf8', timeout: 15_000,
    });
    assert.ifError(child.error);
    assert.equal(child.status, outcome === 'unavailable' ? 1 : 0, child.stderr);
    assert.match(child.stdout, /remaining database pools=0/);
    if (outcome === 'populated') assert.match(child.stdout, /already populated, skipping/);
    if (outcome === 'empty') assert.match(child.stdout, /historical link backfill: queued 0 packets/);
    if (outcome === 'unavailable') assert.match(child.stderr, /fixture database unavailable/);
  });
}
