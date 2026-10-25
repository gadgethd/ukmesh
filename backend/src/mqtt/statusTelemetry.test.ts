import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { persistStatusTelemetrySample, type NodeStatusSampleInput } from './statusTelemetry.js';

const NODE_ID = 'A'.repeat(64);

// Sanitized fixture follows the actual firmware's full-statistics LWT builder:
// https://github.com/gadgethd/MeshCore-MQTT/blob/f7f4b9f1a32a8406149f91185ed63ee0805fa805/examples/simple_repeater/MqttReporter.cpp#L990
const firmwareWill = JSON.parse(await readFile(new URL('./fixtures/firmware-will.json', import.meta.url), 'utf8'));

test('firmware-shaped Will never reaches the sample writer on public or test ingest', async () => {
  for (const network of ['ukmesh', 'test']) {
    for (const status of ['offline', ' OFFLINE ']) {
      const written = await persistStatusTelemetrySample(
        { ...firmwareWill, status },
        { nodeId: NODE_ID, network, allowRawStatsOnly: network === 'test' },
        async () => { assert.fail('Will must not be inserted, including on retained replay'); },
      );
      assert.equal(written, false);
    }
  }
});

test('the same firmware statistics are valid live telemetry, including zero counters', async () => {
  for (const status of ['online', undefined]) {
    const writes: NodeStatusSampleInput[] = [];
    assert.equal(await persistStatusTelemetrySample(
      { ...firmwareWill, status },
      { nodeId: NODE_ID, network: 'ukmesh' },
      async sample => { writes.push(sample); },
    ), true);
    assert.deepEqual(writes, [{
      nodeId: NODE_ID, network: 'ukmesh', status,
      batteryMv: 4200, uptimeSecs: 1, txAirSecs: 0, rxAirSecs: 0,
      channelUtilization: 0, airUtilTx: 0, stats: firmwareWill.stats,
    }]);
  }
});

test('Will rejection precedes both raw owner stats and the test raw-stats fallback', async () => {
  for (const stats of [{ mqtt: { connected: false } }, { unknown_counter: 1 }]) {
    assert.equal(await persistStatusTelemetrySample(
      { ...firmwareWill, stats },
      { nodeId: NODE_ID, network: 'test', allowRawStatsOnly: true },
      async () => { assert.fail('raw-stats fallback must not admit Wills'); },
    ), false);
    assert.equal(await persistStatusTelemetrySample(
      { ...firmwareWill, stats, status: 'online' },
      { nodeId: NODE_ID, network: 'test', allowRawStatsOnly: true },
      async sample => { assert.deepEqual(sample.stats, stats); },
    ), true);
  }
});

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
