import assert from 'node:assert/strict';
import test from 'node:test';
import { metricsRegistry, setWorkerHeartbeatCollector, workerHeartbeatAgeSeconds } from '../metrics.js';
import {
  LINK_WORKER_HEARTBEAT_KEY,
  linkWorkerHeartbeatCollector,
  readLinkWorkerHeartbeatAge,
  workerHeartbeatAge,
} from './workerHeartbeat.js';

test('worker heartbeat ages use Unix seconds and validate missing or corrupt readings', () => {
  assert.equal(workerHeartbeatAge('990', 1_000), 10);
  assert.equal(workerHeartbeatAge('1001', 1_000), 0, 'small clock skew is tolerated');
  assert.equal(workerHeartbeatAge('750', 1_000), 250);
  for (const value of [null, '', ' ', 'garbage', 'NaN', 'Infinity', '0', '-1', '1061']) {
    assert.equal(workerHeartbeatAge(value, 1_000), -1, String(value));
  }
});

test('link gauge reads the live Redis heartbeat even when ITM writes are fifteen minutes old', async () => {
  const keys: string[] = [];
  const client = {
    async get(key: string) { keys.push(key); return '995'; },
  };
  assert.equal(await readLinkWorkerHeartbeatAge(client, () => 1_000_000), 5);
  assert.deepEqual(keys, ['meshcore:link:v3:worker_heartbeat']);
  assert.equal(LINK_WORKER_HEARTBEAT_KEY, keys[0]);
});

test('expiry and Redis failure explicitly replace previously healthy heartbeat values', async () => {
  assert.equal(await readLinkWorkerHeartbeatAge({ get: async () => null }), -1);
  assert.equal(await readLinkWorkerHeartbeatAge({ get: async () => { throw new Error('Redis unavailable'); } }), -1);
});

test('a stalled Redis read cannot hang the metrics scrape', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const reading = readLinkWorkerHeartbeatAge({ get: () => new Promise(() => {}) });
  t.mock.timers.tick(2_000);
  assert.equal(await reading, -1);
});

test('each metrics scrape collects a fresh heartbeat and reflects expiry and recovery', async (t) => {
  t.after(() => { setWorkerHeartbeatCollector(null); workerHeartbeatAgeSeconds.reset(); });
  let timestamp: string | null = '990';
  let clockMs = 1_000_000;
  let reads = 0;
  setWorkerHeartbeatCollector(linkWorkerHeartbeatCollector(
    () => ({ get: async () => { reads++; return timestamp; } }),
    workerHeartbeatAgeSeconds,
    () => clockMs,
  ));
  assert.match(await metricsRegistry.metrics(), /\{worker="link"\} 10\n/);
  clockMs += 15_000;
  assert.match(await metricsRegistry.metrics(), /\{worker="link"\} 25\n/);
  timestamp = null;
  assert.match(await metricsRegistry.metrics(), /\{worker="link"\} -1\n/);
  timestamp = '1015';
  assert.match(await metricsRegistry.metrics(), /\{worker="link"\} 0\n/);
  assert.equal(reads, 4);
});

test('Redis client initialization failure exports an unknown heartbeat and the next scrape recovers', async (t) => {
  t.after(() => { setWorkerHeartbeatCollector(null); workerHeartbeatAgeSeconds.reset(); });
  let initializationFails = true;
  workerHeartbeatAgeSeconds.set({ worker: 'link' }, 0);
  setWorkerHeartbeatCollector(linkWorkerHeartbeatCollector(
    () => {
      if (initializationFails) throw new Error('Redis client initialization failed');
      return { get: async () => '995' };
    },
    workerHeartbeatAgeSeconds,
    () => 1_000_000,
  ));
  assert.match(await metricsRegistry.metrics(), /\{worker="link"\} -1\n/);
  initializationFails = false;
  assert.match(await metricsRegistry.metrics(), /\{worker="link"\} 5\n/);
});
