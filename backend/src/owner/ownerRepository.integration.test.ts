import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import test from 'node:test';
import { promisify } from 'node:util';
import { createOwnerRepository } from './ownerRepository.js';

// SELECT-only fixtures shadow every source relation. No data or DDL writes.
const sqlTest = process.env['OWNER_SQL_TESTS'] === '1' ? test : test.skip;
const run = promisify(execFile);
const owner = 'A'.repeat(64), alias = 'F'.repeat(64), peer = 'B'.repeat(64), unrelated = 'C'.repeat(64);
const literal = (value: unknown) => `'${String(value).replace(/'/g, "''")}'`;
const packet = (overrides: Record<string, unknown> = {}) => ({ age: 1, rx_node_id_raw: owner, src_node_id: peer, packet_hash: 'hash', packet_type: 4, route_type: 1, hop_count: 0, rssi: -100, snr: 5, payload: {}, ...overrides });
const sample = (overrides: Record<string, unknown> = {}) => ({ age: 1, node_id: owner, network: 'ukmesh', neighbors: [{ id: peer }], stats: { battery_mv: 4100 }, ...overrides });

async function execute(index: number, fixtures: { packets?: unknown[]; samples?: unknown[] }, requested = owner) {
  const queries: string[] = [];
  await createOwnerRepository({ query: async text => { queries.push(text); return { rows: [] }; } }).fetchOwnerLiveData(requested);
  // Resolve only the selected test key without reading production mappings.
  const canonical = requested.trim().toUpperCase() === alias ? owner : requested.trim().toUpperCase();
  const sql = queries[index]!.replaceAll('meshcore_canonical_node_id($1)', literal(canonical)).replaceAll('$1', literal(requested));
  const fixture = `WITH
    node_identity_aliases AS (SELECT ${literal(alias)}::text AS source_node_id, ${literal(owner)}::text AS canonical_node_id),
    node_identity_nodes AS (
      SELECT node_id, 'fixture-' || LEFT(node_id, 1) AS name, 'ukmesh'::text AS network,
        'MME'::text AS iata, 54.5::double precision AS lat, -1.3::double precision AS lon
      FROM unnest(ARRAY[${literal(owner)},${literal(peer)}]::text[]) AS n(node_id)
    ), node_identity_packets AS (
      SELECT p.*, NOW() - age * INTERVAL '1 second' AS time
      FROM jsonb_to_recordset(${literal(JSON.stringify(fixtures.packets ?? []))}::jsonb) AS p(
        age double precision, rx_node_id_raw text, src_node_id text, packet_hash text,
        packet_type integer, route_type integer, hop_count integer, payload jsonb,
        rssi double precision, snr double precision
      )
    ), fixture_samples AS (
      SELECT s.*, NOW() - age * INTERVAL '1 second' AS time
      FROM jsonb_to_recordset(${literal(JSON.stringify(fixtures.samples ?? []))}::jsonb) AS s(
        age double precision, node_id text, network text, neighbors jsonb, stats jsonb,
        battery_mv double precision, uptime_secs bigint, tx_air_secs double precision,
        rx_air_secs double precision, channel_utilization double precision, air_util_tx double precision
      )
    ), node_neighbor_samples AS (SELECT * FROM fixture_samples),
    node_status_samples AS (SELECT * FROM fixture_samples)
    SELECT COALESCE(jsonb_agg(to_jsonb(result)), '[]'::jsonb) FROM (${sql}) result;`;
  const child = run('docker', ['exec', '-i', 'meshcore-infra-timescaledb-1', 'psql', '-X', '-U', 'meshcore', '-d', 'meshcore', '-At', '-v', 'ON_ERROR_STOP=1'], { timeout: 20_000, maxBuffer: 4 * 1024 * 1024 });
  child.child.stdin!.end(fixture);
  return JSON.parse((await child).stdout) as Array<Record<string, any>>;
}

sqlTest('packet feed merges aliases, deduplicates newest hashes and caps nine before enrichment', async () => {
  const rows = await execute(2, { packets: [
    ...Array.from({ length: 12 }, (_, i) => packet({ age: i + 10, packet_hash: String(i), rx_node_id_raw: i % 2 ? alias : owner })),
    packet({ age: 1, packet_hash: '0', rx_node_id_raw: alias, payload: { decrypted: { text: 'latest duplicate', sender: 'Alice' } } }),
    packet({ age: 0, packet_hash: 'unrelated', rx_node_id_raw: unrelated }), packet({ age: 31 * 86400, packet_hash: 'expired' }),
  ] });
  assert.equal(rows.length, 9);
  assert.deepEqual(rows.map(row => row.packet_hash), ['0','1','2','3','4','5','6','7','8']);
  assert.equal(rows[0]!.body, 'latest duplicate');
  assert.equal(rows[0]!.sender, 'Alice');
  assert.equal(rows[0]!.src_node_name, 'fixture-B');
  assert.deepEqual(await execute(2, { packets: [packet()] }, unrelated), []);
});

sqlTest('packet budget is global across aliases and null hashes retain fallback deduplication', async () => {
  const rows = await execute(2, { packets: [
    ...Array.from({ length: 5000 }, (_, i) => packet({ age: i / 100, packet_hash: 'duplicate', rx_node_id_raw: i % 2 ? alias : owner })),
    packet({ age: 100, packet_hash: 'outside-global-budget' }),
  ] });
  assert.deepEqual(rows.map(row => row.packet_hash), ['duplicate']);
  const nulls = await execute(2, { packets: [packet({ packet_hash: null }), packet({ packet_hash: null, rx_node_id_raw: alias }), packet({ packet_hash: null, payload: { decrypted: { text: 'different' } } })] });
  assert.equal(nulls.length, 2);
});

sqlTest('neighbours select newest alias report, preserve empty reports and bound time and network', async () => {
  const rows = await execute(7, { samples: [sample({ age: 50 }), sample({ node_id: alias, age: 5, neighbors: [] }), sample({ node_id: unrelated, age: 0 }), sample({ network: 'test', age: 0 }), sample({ age: 8 * 86400 })] }, alias);
  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0]!.neighbors, []);
  assert.deepEqual(await execute(7, { samples: [sample({ age: 7 * 86400 }), sample({ network: 'test' }), sample({ node_id: unrelated })] }), []);
});

sqlTest('telemetry keeps raw identities in 24 hours, chronological order and nullable fields', async () => {
  const rows = await execute(6, { samples: [sample({ age: 50, stats: { battery_mv: 4050 } }), sample({ age: 10, node_id: alias, stats: {}, battery_mv: 4200 }), sample({ age: 1, node_id: alias, stats: { battery_mv: 'unknown' } }), sample({ age: 86400 }), sample({ node_id: unrelated }), sample({ network: 'test' })] });
  assert.deepEqual(rows.map(row => row.battery_mv), [4050, 4200, null]);
  assert.ok(Date.parse(rows[0]!.time) < Date.parse(rows[2]!.time));
  assert.deepEqual(await execute(6, { samples: [sample()] }, unrelated), []);
});

sqlTest('direct evidence excludes relays, self, tx reports, unrelated receivers and expired packets', async () => {
  const rows = await execute(1, { packets: [packet(), packet({ rx_node_id_raw: alias, rssi: -90, snr: 1 }), packet({ hop_count: 1 }), packet({ src_node_id: owner }), packet({ payload: { direction: 'tx' } }), packet({ rx_node_id_raw: unrelated }), packet({ age: 86400 })] });
  assert.equal(rows.length, 1);
  assert.equal(rows[0]!.packets_24h, 2);
  assert.equal(rows[0]!.avg_rssi, -95);
  assert.equal(rows[0]!.avg_snr, 3);
});
