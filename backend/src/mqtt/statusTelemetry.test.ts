import assert from 'node:assert/strict';
import test from 'node:test';
import { persistStatusTelemetrySample, type NodeStatusSampleInput } from './statusTelemetry.js';

const NODE_ID = 'A'.repeat(64);

test('offline Will telemetry is skipped, including a retained replay with boot-fresh stats', async () => {
  const writes: NodeStatusSampleInput[] = [];
  const written = await persistStatusTelemetrySample(
    {
      status: 'offline',
      stats: { battery_mv: '4200', uptime_secs: '0', boot_count: '1' },
    },
    { nodeId: NODE_ID, network: 'ukmesh' },
    async (sample) => { writes.push(sample); },
  );

  assert.equal(written, false);
  assert.deepEqual(writes, []);
});

test('real telemetry is stored with its top-level status', async () => {
  const writes: NodeStatusSampleInput[] = [];
  const written = await persistStatusTelemetrySample(
    {
      status: 'online',
      stats: { battery_mv: '4100', uptime_secs: '7200' },
    },
    { nodeId: NODE_ID, network: 'ukmesh' },
    async (sample) => { writes.push(sample); },
  );

  assert.equal(written, true);
  assert.equal(writes.length, 1);
  assert.equal(writes[0]?.status, 'online');
  assert.equal(writes[0]?.nodeId, NODE_ID);
  assert.equal(writes[0]?.network, 'ukmesh');
  assert.equal(writes[0]?.batteryMv, 4100);
  assert.equal(writes[0]?.uptimeSecs, 7200);
  assert.deepEqual(writes[0]?.stats, { battery_mv: '4100', uptime_secs: '7200' });
});
