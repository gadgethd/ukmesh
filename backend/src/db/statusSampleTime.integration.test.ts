import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';

const url = process.env['TEST_STATUS_DATABASE_URL'];
const local = url && ['localhost', '127.0.0.1'].includes(new URL(url).hostname);

test('the production status writer preserves replay time without regressing presence', {
  skip: local ? false : 'loopback TEST_STATUS_DATABASE_URL fixture required',
}, async () => {
  const connection = new pg.Client({ connectionString: url });
  await connection.connect();
  let close: (() => Promise<void>) | undefined;
  try {
    await connection.query(`CREATE SCHEMA IF NOT EXISTS core;
      CREATE TABLE IF NOT EXISTS core.nodes (node_id text PRIMARY KEY, last_status_at timestamptz,
        last_seen timestamptz, is_online boolean);
      CREATE TABLE IF NOT EXISTS core.node_status_samples (time timestamptz, node_id text, network text,
        battery_mv integer, uptime_secs bigint, tx_air_secs bigint, rx_air_secs bigint,
        channel_utilization float, air_util_tx float, stats jsonb, status text);`);
    process.env['DATABASE_URL'] = url;
    const db = { ...await import('../repositories/nodeState.js'), ...await import('./runtime.js') };
    close = db.closeDb;
    const nodeId = 'A'.repeat(64);
    const existing = new Date(Date.now() - 3_600_000).toISOString();
    const observedAt = new Date(Date.now() - 4 * 3_600_000).toISOString();
    await connection.query(`INSERT INTO core.nodes VALUES ($1, $2, $2, false)`, [nodeId, existing]);
    await db.insertNodeStatusSample({ nodeId, network: 'ukmesh', observedAt, uptimeSecs: 50 });
    const old = await connection.query(`SELECT last_status_at, last_seen, is_online FROM core.nodes`);
    assert.equal(old.rows[0].last_status_at.toISOString(), existing);
    assert.equal(old.rows[0].last_seen.toISOString(), existing);
    assert.equal(old.rows[0].is_online, false);
    const samples = await connection.query(`SELECT time FROM core.node_status_samples`);
    assert.equal(samples.rows[0].time.toISOString(), observedAt);
    const fresh = new Date().toISOString();
    await db.insertNodeStatusSample({ nodeId, network: 'ukmesh', observedAt: fresh, uptimeSecs: 1 });
    const current = await connection.query(`SELECT last_status_at, is_online FROM core.nodes`);
    assert.equal(current.rows[0].last_status_at.toISOString(), fresh);
    assert.equal(current.rows[0].is_online, true);
  } finally {
    if (close) await close();
    await connection.query('DROP TABLE IF EXISTS core.nodes, core.node_status_samples CASCADE');
    await connection.end();
  }
});
