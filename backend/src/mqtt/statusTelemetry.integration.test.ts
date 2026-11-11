import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { createDurableMqttHandleMessage } from './durableHandler.js';
import { persistStatusTelemetrySample, persistStatusWrites } from './statusTelemetry.js';

const databaseUrl = process.env['TEST_STATUS_DATABASE_URL'];

test('PostgreSQL accepts fractional status telemetry and a poison status cannot block subsequent packets', {
  skip: databaseUrl ? false : 'TEST_STATUS_DATABASE_URL is not configured',
}, async () => {
  // Session-local tables only: this test never writes application tables.
  const client = new pg.Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    await client.query(`CREATE TEMPORARY TABLE status_telemetry_fixture (
      battery_mv INTEGER, uptime_secs BIGINT, tx_air_secs BIGINT, rx_air_secs BIGINT, stats JSONB
    )`);
    await client.query('CREATE TEMPORARY TABLE status_packet_fixture (id INTEGER)');
    const rejected: string[] = [];
    const handle = createDurableMqttHandleMessage(async (packet) => {
      if (packet.topic.endsWith('/status')) {
        await persistStatusWrites([
          persistStatusTelemetrySample(JSON.parse(packet.payload.toString()), {
            nodeId: 'A'.repeat(64), network: 'ukmesh',
          }, async (sample) => {
            await client.query(`INSERT INTO status_telemetry_fixture VALUES ($1, $2, $3, $4, $5::jsonb)`, [
              sample.batteryMv ?? null, sample.uptimeSecs ?? null, sample.txAirSecs ?? null,
              sample.rxAirSecs ?? null, JSON.stringify(sample.stats),
            ]);
          }),
        ], (code) => { rejected.push(code); });
      } else {
        await client.query('INSERT INTO status_packet_fixture VALUES (1)');
      }
    }, () => assert.fail('status data must not disconnect the intake'));
    const deliver = (suffix: string, payload: object) => new Promise<void>((resolve, reject) => {
      handle({ topic: `meshcore/MME/${'A'.repeat(64)}/${suffix}`, payload: JSON.stringify(payload) },
        (error) => error ? reject(error) : resolve());
    });
    const stats = { battery_mv: '4100.9', uptime_secs: '7200.9', tx_air_secs: 1.9025919999999998, rx_air_secs: 27.746304 };
    await deliver('status', { status: 'online', stats });
    const result = await client.query('SELECT * FROM status_telemetry_fixture');
    assert.equal(result.rows.length, 1);
    assert.deepEqual(result.rows[0], {
      battery_mv: 4100, uptime_secs: '7200', tx_air_secs: '1', rx_air_secs: '27', stats,
    });
    // PostgreSQL JSONB rejects a decoded NUL even though it is valid JSON.
    await deliver('status', { stats: { battery_mv: 4100, reset_reason: '\u0000' } });
    assert.deepEqual(rejected, ['22P05']);
    await deliver('packets', {});
    assert.equal((await client.query('SELECT COUNT(*)::integer AS count FROM status_packet_fixture')).rows[0].count, 1);
    await deliver('status', { stats: { battery_mv: '1e99', uptime_secs: -1, tx_air_secs: '1e99', rx_air_secs: 0 } });
    assert.equal((await client.query('SELECT COUNT(*)::integer AS count FROM status_telemetry_fixture')).rows[0].count, 2);
  } finally {
    await client.end();
  }
});
