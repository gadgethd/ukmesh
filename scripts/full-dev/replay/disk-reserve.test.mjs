import test from 'node:test';
import assert from 'node:assert/strict';
import { replayDiskReserve } from './disk-reserve.mjs';

const reserve = 10 * 1024 ** 3;
const margin = 512 * 1024 ** 2;

test('blocks publication below the disk reserve and resumes after headroom returns', async () => {
  let now = 0;
  let available = reserve + margin;
  let checks = 0;
  const permits = replayDiskReserve(reserve, async () => { checks++; return available; }, () => now);
  assert.equal(await permits(), true);
  available = reserve - 1;
  assert.equal(await permits(), true);
  assert.equal(checks, 1);
  now += 2_000;
  assert.equal(await permits(), false);
  available = reserve + margin - 1;
  now += 2_000;
  assert.equal(await permits(), false);
  available++;
  now += 2_000;
  assert.equal(await permits(), true);
});

test('pauses replay when it cannot determine available disk space', async () => {
  const failed = replayDiskReserve(reserve, async () => { throw new Error('unavailable'); });
  const invalid = replayDiskReserve(reserve, async () => Number.NaN);
  assert.equal(await failed(), false);
  assert.equal(await invalid(), false);
});
