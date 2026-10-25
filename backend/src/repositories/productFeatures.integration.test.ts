import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import test from 'node:test';
import { promisify } from 'node:util';
import { linkHistoryRows, type QueryFn } from './productFeatures.js';

// Opt-in, read-only PostgreSQL checks. Every relation used by the production
// query is shadowed by a VALUES/JSON fixture CTE; no tables or rows are written.
// LINK_HISTORY_SQL_TESTS=1 node --import tsx --test src/repositories/productFeatures.integration.test.ts
const sqlTest = process.env['LINK_HISTORY_SQL_TESTS'] === '1' ? test : test.skip;
const execFileAsync = promisify(execFile);
const nodeA = 'A'.repeat(64);
const nodeB = 'B'.repeat(64);
const aliasA = 'F'.repeat(64);
const aliasB = '2'.repeat(64);
const unrelated = 'C'.repeat(64);
const baseTime = Date.now() - 2 * 60 * 60_000;

type Packet = {
  time: string;
  observation_id: string;
  rx_node_id: string | null;
  src_node_id: string | null;
  network: string;
  snr: number | null;
  rssi: number | null;
  payload: { direction?: string } | null;
  visibility_ok: boolean | null;
  is_private: boolean | null;
  hop_count: number;
};

function packet(index: number, overrides: Partial<Packet> = {}): Packet {
  return {
    time: new Date(baseTime + index * 1_000).toISOString(),
    observation_id: `00000000-0000-0000-0000-${String(index).padStart(12, '0')}`,
    rx_node_id: nodeA,
    src_node_id: nodeB,
    network: 'ukmesh',
    snr: -index,
    rssi: -99,
    payload: { direction: 'rx' },
    visibility_ok: true,
    is_private: false,
    hop_count: 0,
    ...overrides,
  };
}

function literal(value: unknown): string {
  if (Array.isArray(value)) return `ARRAY[${value.map(literal).join(',')}]::text[]`;
  return `'${String(value).replace(/'/g, "''")}'`;
}

async function history(
  packets: Packet[],
  options: { nodeA?: string; nodeB?: string; hours?: number; networks?: string[]; noPathLoss?: boolean; stalePrivacy?: boolean } = {},
) {
  const aliases = [
    { source_node_id: aliasA, canonical_node_id: nodeA },
    { source_node_id: aliasB, canonical_node_id: nodeB },
  ];
  const links = options.noPathLoss ? [] : [
    { node_a_id: nodeA, node_b_id: nodeB, itm_path_loss_db: 140 },
    { node_a_id: nodeB, node_b_id: aliasA, itm_path_loss_db: 110 },
    { node_a_id: aliasB, node_b_id: nodeA, itm_path_loss_db: 120 },
    { node_a_id: nodeA, node_b_id: unrelated, itm_path_loss_db: 10 },
  ];
  const query: QueryFn = async (sql, params) => {
    const bound = sql.replace(/\$(\d+)\b/g, (_placeholder, index: string) => literal(params![Number(index) - 1]));
    const fixtureSql = `WITH
      packets AS (
        SELECT * FROM jsonb_to_recordset(${literal(JSON.stringify(packets))}::jsonb) AS p(
          time timestamptz, observation_id uuid, rx_node_id text, src_node_id text,
          network text, snr double precision, rssi double precision, payload jsonb,
          visibility_ok boolean, is_private boolean, hop_count integer
        )
      ), node_identity_aliases AS (
        SELECT * FROM jsonb_to_recordset(${literal(JSON.stringify(aliases))}::jsonb)
          AS a(source_node_id text, canonical_node_id text)
      ), node_links AS (
        SELECT * FROM jsonb_to_recordset(${literal(JSON.stringify(links))}::jsonb)
          AS l(node_a_id text, node_b_id text, itm_path_loss_db double precision)
      ), packet_visibility_materialization_state AS (
        SELECT TRUE AS singleton, 1 AS visibility_generation
      ), public_visibility_state AS (
        SELECT TRUE AS singleton, ${options.stalePrivacy ? 2 : 1} AS generation
      )
      SELECT COALESCE(jsonb_agg(to_jsonb(points)), '[]'::jsonb)
      FROM (${bound}) points;`;
    // Use stdin to keep large fixtures within the OS argument-size limit.
    const child = execFileAsync('docker', [
      'exec', '-i', 'meshcore-infra-timescaledb-1',
      'psql', '-X', '-U', 'meshcore', '-d', 'meshcore', '-v', 'ON_ERROR_STOP=1', '-At',
    ], { maxBuffer: 2 * 1024 * 1024, timeout: 20_000 });
    child.child.stdin!.end(fixtureSql);
    const { stdout } = await child;
    return { rows: JSON.parse(stdout) };
  };
  return (await linkHistoryRows(
    query, options.nodeA ?? nodeA, options.nodeB ?? nodeB, options.hours ?? 72,
    options.networks ?? ['ukmesh', 'northeast', 'teesside'],
  )).rows;
}

sqlTest('packet history expands both aliases, includes relayed receptions and preserves ISO radio points', async () => {
  const packets = [
    packet(1),
    packet(2, { rx_node_id: nodeB, src_node_id: nodeA }),
    packet(3, { rx_node_id: aliasA, src_node_id: aliasB, hop_count: 4 }),
    packet(4, { rx_node_id: aliasB, src_node_id: aliasA, hop_count: 2 }),
  ];
  const expected = packets.map(({ time, snr, rssi }) => ({ time, snr, rssi, path_loss: 110, sample_count: 1 }));
  assert.deepEqual(await history(packets), expected);
  assert.deepEqual(await history(packets, { nodeA: aliasA.toLowerCase(), nodeB: ` ${aliasB} ` }), expected);
  assert.deepEqual(await history(packets, { nodeA: nodeB, nodeB: nodeA }), expected);
});

sqlTest('packet history excludes null sources, unrelated peers, TX, absent metrics and hidden receptions', async () => {
  const good = packet(1, { payload: null, snr: 0, rssi: null });
  const rssiOnly = packet(2, { snr: null });
  const rows = await history([
    good, rssiOnly,
    packet(3, { src_node_id: null }),
    packet(4, { src_node_id: unrelated }),
    packet(5, { rx_node_id: unrelated }),
    packet(6, { rx_node_id: null }),
    packet(7, { payload: { direction: 'tx' } }),
    packet(8, { snr: null, rssi: null }),
    packet(9, { visibility_ok: false }),
    packet(10, { visibility_ok: null }),
    packet(11, { is_private: true }),
  ]);
  assert.deepEqual(rows.map(({ time, snr, rssi }) => ({ time, snr, rssi })),
    [good, rssiOnly].map(({ time, snr, rssi }) => ({ time, snr, rssi })));
  assert.deepEqual(await history([good], { stalePrivacy: true }), []);
});

sqlTest('packet history enforces network isolation and the requested time window', async () => {
  const packets = [
    packet(1),
    packet(2, { network: 'northeast' }),
    packet(3, { network: 'teesside' }),
    packet(4, { network: 'test' }),
    packet(5, { time: new Date(Date.now() - 73 * 60 * 60_000).toISOString() }),
    packet(6, { time: new Date(Date.now() + 60 * 60_000).toISOString() }),
  ];
  assert.deepEqual((await history(packets)).map(({ snr }) => snr), [-1, -2, -3]);
  assert.deepEqual((await history(packets, { networks: ['test'] })).map(({ snr }) => snr), [-4]);
  assert.deepEqual(await history(packets, { hours: 1 }), []);
  assert.deepEqual((await history(packets, { hours: 168 })).map(({ snr }) => snr), [-5, -1, -2, -3]);
});

sqlTest('packet history returns the latest 1000 observations in chronological order', async () => {
  const packets = Array.from({ length: 1005 }, (_, index) => packet(index));
  const rows = await history(packets.reverse());
  assert.equal(rows.length, 1000);
  assert.equal(rows[0].snr, -5);
  assert.equal(rows.at(-1)!.snr, -1004);
  assert.deepEqual(rows.map(({ time }) => time), [...rows.map(({ time }) => time)].sort());
  assert.ok(rows.every((row) => row.sample_count === 1));
});

sqlTest('packet history keeps radio samples without a model link and returns empty for no traffic', async () => {
  assert.deepEqual(await history([packet(1)], { noPathLoss: true }), [{
    time: packet(1).time, snr: -1, rssi: -99, path_loss: null, sample_count: 1,
  }]);
  assert.deepEqual(await history([]), []);
  assert.deepEqual(await history([packet(1)], { nodeB: unrelated }), []);
});
