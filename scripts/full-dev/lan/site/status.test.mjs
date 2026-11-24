import assert from 'node:assert/strict';
import test from 'node:test';
import { createStatusReader } from './status.mjs';

test('concurrent dashboard requests share one bounded refresh', async () => {
  let time = 1000;
  let calls = 0;
  const releases = [];
  const status = createStatusReader(
    [{ id: 'mesh', name: 'App', port: 8081, group: 'main', note: 'Map', access: 'app', origin: 'app',
      upstream: 'http://app', probe: '/readyz', operator_paths: ['/api/example'] }],
    async (endpoint) => {
      assert.equal(endpoint, 'http://app/readyz');
      calls++;
      await new Promise((resolve) => releases.push(resolve));
      return { up: true, status: 200 };
    },
    () => time,
  );
  const first = status();
  const concurrent = status();
  assert.equal(calls, 1);
  releases.shift()();
  const result = await first;
  assert.deepEqual(result, await concurrent);
  assert.deepEqual(result.targets, [{ id: 'mesh', name: 'App', port: 8081, group: 'main', note: 'Map', up: true, status: 200 }]);
  time += 4999;
  assert.deepEqual(await status(), result);
  time++;
  const refreshed = status();
  assert.equal(calls, 2);
  releases.shift()();
  assert.notEqual((await refreshed).at, result.at);
});
