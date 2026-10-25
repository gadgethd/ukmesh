import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import test from 'node:test';
import { promisify } from 'node:util';
import { networkFilters } from '../api/utils/networkFilters.js';
import { createNodeRepository } from './nodes.js';

// Opt-in PostgreSQL fixture execution: SELECT only, with all production
// relations shadowed by CTEs. No tables, functions or data are written.
const sqlTest = process.env['NODE_LINKS_SQL_TESTS'] === '1' ? test : test.skip;
const execFileAsync = promisify(execFile);
const source = 'A'.repeat(64);
const peer = 'B'.repeat(64);
const quietPeer = 'C'.repeat(64);
const sourceAlias = 'F'.repeat(64);
const peerAlias = '2'.repeat(64);
const unrelated = 'D'.repeat(64);

function packet(overrides: Record<string, unknown> = {}) {
  return {
    seconds_ago: 3600, rx_node_id: source, src_node_id: peer,
    network: 'ukmesh', topic: 'ukmesh/MME/observer/packets', topic_prefix: 'ukmesh',
    visibility_ok: true, is_private: false, packet_hash: 'same-hash',
    hop_count: 4, snr: null, rssi: null, payload: { direction: 'rx' },
    ...overrides,
  };
}

function literal(value: unknown): string {
  if (Array.isArray(value)) return `ARRAY[${value.map(literal).join(',')}]::text[]`;
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function links(packets: ReturnType<typeof packet>[], options: {
  request?: string; network?: string; stalePrivacy?: boolean;
  sourceName?: string; peerName?: string; peerNetwork?: string; sighted?: boolean;
} = {}) {
  const nodes = [
    { node_id: source, name: options.sourceName ?? 'Source', network: 'ukmesh' },
    { node_id: peer, name: options.peerName ?? 'Peer', network: options.peerNetwork ?? 'ukmesh' },
    { node_id: quietPeer, name: 'Quiet', network: 'ukmesh' },
    { node_id: unrelated, name: 'Test', network: 'test' },
  ];
  const aliases = [
    { source_node_id: sourceAlias, canonical_node_id: source },
    { source_node_id: peerAlias, canonical_node_id: peer },
  ];
  const identityLinks = [
    { node_a_id: source, node_b_id: peer, observed_count: 9999,
      itm_viable: true, force_viable: false, itm_path_loss_db: 112.1,
      count_a_to_b: 99, count_b_to_a: 9900, last_observed: '2026-09-01T12:34:56.789+01:00' },
    { node_a_id: quietPeer, node_b_id: source, observed_count: 50000,
      itm_viable: false, force_viable: true, itm_path_loss_db: null,
      count_a_to_b: 40000, count_b_to_a: 10000, last_observed: null },
    { node_a_id: source, node_b_id: unrelated, observed_count: 3,
      itm_viable: false, force_viable: false, itm_path_loss_db: null,
      count_a_to_b: 2, count_b_to_a: 1, last_observed: null },
  ];
  const calls: string[] = [];
  const repo = createNodeRepository(async (sql, params) => {
    calls.push(sql);
    const bound = sql.replace(/\$(\d+)\b/g, (_placeholder, index: string) => literal(params![Number(index) - 1]));
    const fixture = `WITH
      packets AS (
        SELECT p.*, NOW() - seconds_ago * INTERVAL '1 second' AS time
          FROM jsonb_to_recordset(${literal(JSON.stringify(packets))}::jsonb) AS p(
            seconds_ago double precision, rx_node_id text, src_node_id text,
            network text, topic text, topic_prefix text, visibility_ok boolean,
            is_private boolean, packet_hash text, hop_count integer,
            snr double precision, rssi double precision, payload jsonb
          )
      ), node_identity_aliases AS (
        SELECT * FROM jsonb_to_recordset(${literal(JSON.stringify(aliases))}::jsonb)
          AS a(source_node_id text, canonical_node_id text)
      ), node_identity_nodes AS (
        SELECT * FROM jsonb_to_recordset(${literal(JSON.stringify(nodes))}::jsonb)
          AS n(node_id text, name text, network text)
      ), nodes AS (SELECT * FROM node_identity_nodes),
      node_identity_links AS (
        SELECT * FROM jsonb_to_recordset(${literal(JSON.stringify(identityLinks))}::jsonb)
          AS l(node_a_id text, node_b_id text, observed_count integer,
            itm_viable boolean, force_viable boolean, itm_path_loss_db double precision,
            count_a_to_b integer, count_b_to_a integer, last_observed timestamptz)
      ), node_identity_sightings AS (
        SELECT ${literal(peer)}::text AS node_id, 'ukmesh'::text AS network, NOW() AS last_seen_at
        WHERE ${options.sighted ? 'TRUE' : 'FALSE'}
      ), packet_visibility_materialization_state AS (
        SELECT TRUE AS singleton, 1 AS visibility_generation
      ), public_visibility_state AS (
        SELECT TRUE AS singleton, ${options.stalePrivacy ? 2 : 1} AS generation
      )
      SELECT COALESCE(jsonb_agg(to_jsonb(result)), '[]'::jsonb) FROM (${bound}) result;`;
    const child = execFileAsync('docker', [
      'exec', '-i', 'meshcore-infra-timescaledb-1', 'psql', '-X',
      '-U', 'meshcore', '-d', 'meshcore', '-v', 'ON_ERROR_STOP=1', '-At',
    ], { timeout: 20_000, maxBuffer: 2 * 1024 * 1024 });
    child.child.stdin!.end(fixture);
    return { rows: JSON.parse((await child).stdout) };
  });
  return { rows: await repo.listNodeLinks(options.request ?? source, networkFilters(options.network ?? 'ukmesh')), calls };
}

sqlTest('contacts aggregate both alias directions, relayed packets and repeated hashes; preserve ISO, orientation and quiet links', async () => {
  const packets = [packet(), packet({ rx_node_id: peer, src_node_id: source }),
    packet({ rx_node_id: sourceAlias, src_node_id: peerAlias }),
    packet({ rx_node_id: peerAlias, src_node_id: sourceAlias }),
    packet({ payload: { direction: 'tx' } })];
  const result = await links(packets);
  assert.equal(result.rows.length, 2);
  assert.equal(result.rows.find(r => r.peer_id === peer)!.recent_count_72h, 5);
  assert.equal(result.rows.find(r => r.peer_id === peer)!.last_observed, '2026-09-01T11:34:56.789Z');
  assert.deepEqual(result.rows.find(r => r.peer_id === quietPeer), {
    peer_id: quietPeer, peer_name: 'Quiet', observed_count: 50000, itm_path_loss_db: null,
    count_this_to_peer: 10000, count_peer_to_this: 40000, last_observed: null, recent_count_72h: 0,
  });
  assert.deepEqual((await links(packets, { request: ` ${sourceAlias.toLowerCase()} ` })).rows, result.rows);
  assert.equal(result.calls.filter(sql => sql.includes('FROM packets p')).length, 1);
});

sqlTest('contacts enforce exact 72h boundaries, public network compatibility and privacy without requiring radio metrics', async () => {
  const packets = [
    packet({ seconds_ago: 72 * 3600 - 1 }), packet({ seconds_ago: 0 }),
    packet({ network: 'northeast' }), packet({ network: 'teesside' }),
    packet({ seconds_ago: 72 * 3600 }), packet({ seconds_ago: -1 }),
    packet({ src_node_id: null }), packet({ rx_node_id: null }),
    packet({ src_node_id: unrelated }), packet({ rx_node_id: unrelated }),
    packet({ network: 'test' }), packet({ network: 'other' }),
    packet({ topic_prefix: 'meshcore-test' }),
    packet({ topic_prefix: null, topic: 'meshcore-test/MME/observer/packets' }),
    packet({ visibility_ok: false }), packet({ visibility_ok: null }), packet({ is_private: true }),
  ];
  const result = await links(packets);
  assert.equal(result.rows.find(r => r.peer_id === peer)!.recent_count_72h, 4);
  assert.ok((await links(packets, { stalePrivacy: true })).rows.every(r => r.recent_count_72h === 0));
});

sqlTest('contacts do not inherit the sparkline 1000-point cap', async () => {
  const result = await links(Array.from({ length: 1005 }, () => packet()));
  assert.equal(result.rows.find(r => r.peer_id === peer)!.recent_count_72h, 1005);
});

sqlTest('source and peer visibility, force viability and recent network sightings remain intact', async () => {
  assert.equal((await links([packet()], { sourceName: '🚫 Source' })).calls.length, 1);
  assert.deepEqual((await links([packet()], { sourceName: '🚫 Source' })).rows, []);
  assert.deepEqual((await links([packet()], { network: 'test' })).rows, []);
  assert.deepEqual((await links([packet()], { peerName: '🚫 Peer' })).rows.map(r => r.peer_id), [quietPeer]);
  assert.deepEqual((await links([packet()], { peerNetwork: 'other' })).rows.map(r => r.peer_id), [quietPeer]);
  assert.equal((await links([packet()], { peerNetwork: 'other', sighted: true })).rows.find(r => r.peer_id === peer)!.recent_count_72h, 1);
  assert.deepEqual((await links([packet()], { peerNetwork: 'test', sighted: true })).rows.map(r => r.peer_id), [quietPeer]);
});
