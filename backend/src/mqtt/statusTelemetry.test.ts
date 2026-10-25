import assert from 'node:assert/strict';
import test from 'node:test';
import { createDurableMqttHandleMessage } from './durableHandler.js';
import { persistStatusTelemetrySample, persistStatusWrites, type NodeStatusSampleInput } from './statusTelemetry.js';

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

test('fractional firmware airtime and uptime become whole seconds while raw stats retain precision', async () => {
  for (const txAirSecs of [1.9025919999999998, 3.296256, 558.2018560000002]) {
    const stats = {
      battery_mv: '4100.75', uptime_secs: '7200.9',
      tx_air_secs: txAirSecs, rx_air_secs: '27.746304',
      channel_utilization: 2.75, air_util_tx: '0.125',
    };
    const writes: NodeStatusSampleInput[] = [];
    assert.equal(await persistStatusTelemetrySample(
      { status: 'online', stats }, { nodeId: NODE_ID, network: 'ukmesh' },
      async (sample) => { writes.push(sample); },
    ), true);
    assert.equal(writes[0]?.batteryMv, 4100);
    assert.equal(writes[0]?.uptimeSecs, 7200);
    assert.equal(writes[0]?.txAirSecs, Math.trunc(txAirSecs));
    assert.equal(writes[0]?.rxAirSecs, 27);
    assert.equal(writes[0]?.channelUtilization, 2.75);
    assert.equal(writes[0]?.airUtilTx, 0.125);
    assert.deepEqual(writes[0]?.stats, stats);
  }
});

test('counter aliases and millisecond uptime use the same integer normalization', async () => {
  const writes: NodeStatusSampleInput[] = [];
  await persistStatusTelemetrySample(
    { stats: { batteryMv: '4200.9', uptimeMs: '60001.9', txAirSecs: '0.9', rxAirSecs: '1.9' } },
    { nodeId: NODE_ID, network: 'ukmesh' }, async (sample) => { writes.push(sample); },
  );
  assert.equal(writes[0]?.batteryMv, 4200);
  assert.equal(writes[0]?.uptimeSecs, 60);
  assert.equal(writes[0]?.txAirSecs, 0);
  assert.equal(writes[0]?.rxAirSecs, 1);
});

test('unrepresentable counters are omitted from integer columns but kept in raw stats', async () => {
  for (const invalid of [-1, Number.MAX_SAFE_INTEGER + 1, '1e99', 'Infinity', 'not-a-number']) {
    const stats = { battery_mv: invalid, uptime_secs: invalid, tx_air_secs: invalid, rx_air_secs: invalid };
    const writes: NodeStatusSampleInput[] = [];
    await persistStatusTelemetrySample(
      { stats }, { nodeId: NODE_ID, network: 'ukmesh' }, async (sample) => { writes.push(sample); },
    );
    assert.equal(writes.length, 1);
    for (const field of ['batteryMv', 'uptimeSecs', 'txAirSecs', 'rxAirSecs'] as const) {
      assert.equal(writes[0]?.[field], undefined, `${field}: ${invalid}`);
    }
    assert.deepEqual(writes[0]?.stats, stats);
  }
});

test('integer domain boundaries remain valid and battery overflow does not discard other telemetry', async () => {
  for (const batteryMv of [0, 2_147_483_647, 2_147_483_648]) {
    const writes: NodeStatusSampleInput[] = [];
    await persistStatusTelemetrySample(
      { stats: { battery_mv: batteryMv, uptime_secs: 0, tx_air_secs: Number.MAX_SAFE_INTEGER, rx_air_secs: 0 } },
      { nodeId: NODE_ID, network: 'ukmesh' }, async (sample) => { writes.push(sample); },
    );
    assert.equal(writes[0]?.batteryMv, batteryMv > 2_147_483_647 ? undefined : batteryMv);
    assert.equal(writes[0]?.uptimeSecs, 0);
    assert.equal(writes[0]?.txAirSecs, Number.MAX_SAFE_INTEGER);
    assert.equal(writes[0]?.rxAirSecs, 0);
  }
});

test('permanent status data exceptions are counted and allow the next MQTT packet to progress', async () => {
  for (const code of ['22P02', '22003', '22P05', '22001']) {
    const rejected: string[] = [];
    let packets = 0;
    const handle = createDurableMqttHandleMessage(async (packet) => {
      if (packet.topic.endsWith('/status')) {
        await persistStatusWrites([
          Promise.resolve(), Promise.reject(Object.assign(new Error('invalid status value'), { code })),
        ], (value) => { rejected.push(value); });
      } else {
        packets += 1;
      }
    }, () => assert.fail('a deterministic status data error must not disconnect MQTT'));
    const deliver = (topic: string) => new Promise<void>((resolve, reject) => {
      handle({ topic, payload: '{}' }, (error) => error ? reject(error) : resolve());
    });
    await deliver(`meshcore/MME/${NODE_ID}/status`);
    await deliver(`meshcore/MME/${NODE_ID}/packets`);
    assert.deepEqual(rejected, [code]);
    assert.equal(packets, 1);
  }
});

test('database outages, timeouts, schema errors and unknown failures still withhold MQTT acknowledgement', async () => {
  for (const code of ['08006', '57P01', '57014', '42P01', 'ECONNRESET', undefined]) {
    const failure = Object.assign(new Error('persistence failed'), { code });
    let reported: Error | undefined;
    const handle = createDurableMqttHandleMessage(
      () => persistStatusWrites([Promise.reject(failure)], () => assert.fail('must remain retryable')),
      (error) => { reported = error; },
    );
    const error = await new Promise<Error | undefined>((resolve) => {
      handle({ topic: `meshcore/MME/${NODE_ID}/status`, payload: '{}' }, resolve);
    });
    assert.equal(error, failure);
    assert.equal(reported, failure);
  }
});

test('status writes settle together and a permanent error cannot hide a concurrent operational failure', async () => {
  let release!: () => void;
  let settled = false;
  const failure = Object.assign(new Error('database unavailable'), { code: '08006' });
  const rejected: string[] = [];
  const result = persistStatusWrites([
    new Promise<void>((resolve) => { release = resolve; }),
    Promise.reject(Object.assign(new Error('invalid input'), { code: '22P02' })),
    Promise.reject(failure),
  ], (code) => { rejected.push(code); });
  const checked = assert.rejects(result, (error: unknown) => error === failure).then(() => { settled = true; });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(settled, false);
  release();
  await checked;
  assert.deepEqual(rejected, ['22P02']);
});
