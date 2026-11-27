/**
 * Audit regression: packet identity vs. durable decryption identity.
 *
 * `getRecentPackets` and `getRecentMessages` collapse observations to one row per
 * packet identity (UID, falling back to the stored hash) and aggregate counts and
 * tags on that same key. `mesh.packet_decryptions` is keyed by `packet_hash`, so
 * joining it on the representative's exact hash returns NULL for an identity whose
 * only decrypted observation is an alias (same wire content, different observation
 * hash). These tests pin the identity-keyed join.
 *
 * A disposable database with only the columns the readers touch keeps this audit
 * focused on the identity predicate. Separate integration suites exercise the
 * complete migration path.
 */
import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import pg from 'pg';
import { createDisposableDatabase } from './fixtures/disposableDatabase.js';
import type { QueryResultRow } from 'pg';
import { getChannelMessageHistory, getRecentMessages, getRecentPackets } from '../repositories/packetReads.js';
import type { QueryFn } from './packetBatch.js';
import { channelHashesForName } from '../mqtt/channelRegistry.js';
import { resolvePendingSummaries } from '../ram/liveRead.js';

const databaseUrl = process.env['TEST_INGEST_DATABASE_URL'];
const isLoopback = (value?: string): boolean => {
  if (!value) return false;
  try {
    return ['localhost', '127.0.0.1'].includes(new URL(value).hostname);
  } catch {
    return false;
  }
};
const SKIP_REASON = 'loopback TEST_INGEST_DATABASE_URL required for the decryption identity fixture';

/** Columns referenced by getRecentPackets/getRecentMessages and their helpers. */
const FIXTURE_SCHEMA = `
  CREATE SCHEMA core;
  CREATE SCHEMA mesh;
  CREATE SCHEMA ops;

  CREATE TABLE mesh.packets (
    time            timestamptz NOT NULL,
    packet_hash     text NOT NULL,
    packet_uid      bytea,
    observation_id  uuid,
    rx_node_id      text,
    src_node_id     text,
    topic           text,
    topic_prefix    text,
    iata            text,
    packet_type     integer,
    route_type      text,
    hop_count       integer,
    rssi            double precision,
    snr             double precision,
    payload         jsonb NOT NULL DEFAULT '{}'::jsonb,
    advert_count    integer,
    path_hashes     text[],
    path_hash_size_bytes integer,
    network         text,
    transport_codes text[],
    region_scope    text,
    visibility_ok   boolean NOT NULL DEFAULT true,
    is_private      boolean NOT NULL DEFAULT false
  );

  CREATE TABLE mesh.packet_decryptions (
    packet_hash text PRIMARY KEY,
    decrypted   jsonb NOT NULL,
    summary     text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
  );

  CREATE TABLE mesh.message_tags (
    packet_hash text PRIMARY KEY,
    packet_time timestamptz NOT NULL,
    tagged_at   timestamptz NOT NULL DEFAULT now(),
    tags        jsonb NOT NULL,
    confidence  jsonb
  );

  CREATE TABLE core.node_identity_aliases (
    source_node_id    text PRIMARY KEY,
    canonical_node_id text NOT NULL,
    confidence        text NOT NULL,
    reason            text NOT NULL
  );

  CREATE TABLE ops.public_visibility_state (
    singleton  boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    generation bigint NOT NULL DEFAULT 1
  );

  CREATE TABLE ops.packet_visibility_materialization_state (
    singleton             boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    visibility_generation bigint NOT NULL DEFAULT 1
  );

  CREATE TABLE ops.privacy_rematerialization_queue (
    node_id text NOT NULL,
    network text,
    target  text NOT NULL,
    status  text NOT NULL
  );

  INSERT INTO ops.public_visibility_state (generation) VALUES (1);
  INSERT INTO ops.packet_visibility_materialization_state (visibility_generation) VALUES (1);
`;

const PUBLIC_FULL_COLUMNS = [
  'advert_count', 'hop_count', 'iata', 'network', 'observer_iatas',
  'observer_node_ids', 'packet_hash', 'packet_type', 'path_hash_size_bytes',
  'path_hashes', 'region_scope', 'route_type', 'rssi', 'rx_count',
  'rx_node_id', 'snr', 'src_node_id', 'summary', 'tag_confidence', 'tags',
  'time', 'topic', 'topic_prefix', 'transport_codes', 'tx_count',
].sort();

const hashes = (seed: string) => seed.repeat(64 / seed.length);
const uidBytes = (seed: string) => Buffer.from(seed.repeat(64 / seed.length), 'hex');

test('getRecentPackets resolves decryptions from a sibling observation of the same UID', {
  skip: isLoopback(databaseUrl) ? false : SKIP_REASON,
}, async (t) => {
  const fixture = await createDisposableDatabase(t, databaseUrl!, 'meshcore_identity_audit');
  const client = new pg.Client({ connectionString: fixture.url });
  fixture.addCleanup(() => client.end());
  await client.connect();

  await client.query(FIXTURE_SCHEMA);

  const uid = uidBytes('a1');           // one wire content, two observations
  const representative = hashes('c1');  // newest, no decryption of its own
  const alias = hashes('c2');           // older, carries the decryption row
  const own = hashes('c3');             // decryption on the representative itself
  const payloadWins = hashes('c4');     // payload summary still takes precedence
  const other = hashes('c5');           // distinct identity, must stay NULL
  const now = Date.now();
  const insert = `INSERT INTO mesh.packets
    (time, packet_hash, packet_uid, rx_node_id, src_node_id, topic, topic_prefix, iata,
     packet_type, route_type, hop_count, rssi, snr, payload, advert_count, path_hashes,
     path_hash_size_bytes, network, transport_codes, region_scope, visibility_ok, is_private)
    VALUES ($1,$2,$3,$4,NULL,$5,$6,'ZZZ',5,'flood',1,-90.5,6.25,$7::jsonb,NULL,'{}',1,
            'ukmesh','{}','#uk',TRUE,FALSE)`;
  await client.query(insert, [new Date(now - 60_000), representative, uid, 'AB'.repeat(32),
    'meshcore/ukmesh/AB/packets', 'ukmesh', '{"direction":"rx"}']);
  await client.query(insert, [new Date(now - 120_000), alias, uid, 'AB'.repeat(32),
    'meshcore/ukmesh/AB/packets', 'ukmesh', '{"direction":"rx"}']);
  await client.query(insert, [new Date(now - 180_000), own, uidBytes('b2'), 'AB'.repeat(32),
    'meshcore/ukmesh/AB/packets', 'ukmesh', '{"direction":"rx"}']);
  await client.query(insert, [new Date(now - 240_000), payloadWins, uidBytes('b3'), 'AB'.repeat(32),
    'meshcore/ukmesh/AB/packets', 'ukmesh', '{"direction":"rx","_summary":"payload summary"}']);
  await client.query(insert, [new Date(now - 270_000), other, uidBytes('b4'), 'AB'.repeat(32),
    'meshcore/ukmesh/AB/packets', 'ukmesh', '{"direction":"rx"}']);

  // Only the alias observation of the shared UID was ever decrypted.
  const decrypt = `INSERT INTO mesh.packet_decryptions (packet_hash, decrypted, summary)
    VALUES ($1, '{}'::jsonb, $2)`;
  await client.query(decrypt, [alias, 'sibling plaintext']);
  await client.query(decrypt, [own, 'own plaintext']);
  await client.query(decrypt, [payloadWins, 'decrypted but lower precedence']);

  const readQuery: QueryFn = (sql, params) => client.query(sql, params);
  const rows = await getRecentPackets(50, 'ukmesh', undefined, 'full', undefined, readQuery);
  const byHash = new Map(rows.map((row) => [String(row.packet_hash), row]));

  // Pre-fix this is NULL: the representative has no decryption row of its own.
  const representativeRow = byHash.get(representative);
  assert.ok(representativeRow, 'the UID group must surface its representative observation');
  assert.equal(representativeRow.summary, 'sibling plaintext',
    'a decrypted alias sharing the UID must supply the group summary');
  assert.equal(representativeRow.rx_count, 2, 'the group still counts both observations');
  assert.deepEqual(
    Object.keys(representativeRow).sort(),
    PUBLIC_FULL_COLUMNS,
    'identity-keyed decryption resolution must not change the public row shape',
  );

  assert.equal(byHash.has(alias), false,
    'the group still holds one representative observation, not one row per hash');
  assert.equal(byHash.get(own)?.summary, 'own plaintext');
  assert.equal(byHash.get(payloadWins)?.summary, 'payload summary',
    'an ingest payload summary still outranks a durable decryption');
  assert.equal(byHash.get(other)?.summary, null, 'an undecrypted identity stays NULL');

  // Rollback authority must still execute and keep one identity per stored hash.
  const previousAuthority = process.env['PACKET_IDENTITY_READ_AUTHORITY'];
  try {
    process.env['PACKET_IDENTITY_READ_AUTHORITY'] = 'hash';
    const rollbackRows = await getRecentPackets(50, 'ukmesh', undefined, 'full', undefined, readQuery);
    const rollbackByHash = new Map(rollbackRows.map((row) => [String(row.packet_hash), row]));
    assert.equal(rollbackByHash.get(representative)?.summary, null,
      'hash authority must not borrow a sibling observation decryption');
    assert.equal(rollbackByHash.get(alias)?.summary, 'sibling plaintext');
    assert.equal(rollbackByHash.get(alias)?.rx_count, 1);
  } finally {
    if (previousAuthority === undefined) delete process.env['PACKET_IDENTITY_READ_AUTHORITY'];
    else process.env['PACKET_IDENTITY_READ_AUTHORITY'] = previousAuthority;
  }
});

test('recent readers join packet_decryptions on the packet identity key', async () => {
  const previous = process.env['PACKET_IDENTITY_READ_AUTHORITY'];
  const captured: string[] = [];
  const capture: QueryFn = async <T extends QueryResultRow>(text: string) => {
    captured.push(String(text));
    return { rows: [] as T[] };
  };
  try {
    process.env['PACKET_IDENTITY_READ_AUTHORITY'] = 'uid';
    await getRecentPackets(5, 'ukmesh', undefined, 'full', undefined, capture);
    const packetsSql = captured.at(-1)!;
    assert.match(packetsSql, /mesh\.packet_decryptions/, 'recent packets still read durable decryptions');
    assert.match(packetsSql, /JOIN mesh\.packet_decryptions pd ON pd\.packet_hash = p\.packet_hash/,
      'decryptions are resolved over the bounded window, not one representative row');
    assert.match(packetsSql, /DISTINCT ON \(identity_key\) identity_key, summary/,
      'decryptions must be keyed by the packet identity');
    assert.doesNotMatch(packetsSql, /pd\.packet_hash = rp\.packet_hash/,
      'joining the decryption to the representative row reintroduces the NULL-summary defect');

    await getRecentMessages(5, 'ukmesh', undefined, undefined, capture);
    const messagesSql = captured.at(-1)!;
    assert.match(messagesSql, /mesh\.packet_decryptions/);
    assert.match(messagesSql, /JOIN mesh\.packet_decryptions pd ON pd\.packet_hash = p\.packet_hash/);
    assert.match(messagesSql, /DISTINCT ON \(identity_key\) identity_key, summary/);
    assert.doesNotMatch(messagesSql, /pd\.packet_hash = m\.packet_hash/);

    // Rollback authority keeps the pre-P2-10 one-observation-per-identity shape.
    process.env['PACKET_IDENTITY_READ_AUTHORITY'] = 'hash';
    await getRecentPackets(5, 'ukmesh', undefined, 'full', undefined, capture);
    const rollbackSql = captured.at(-1)!;
    assert.doesNotMatch(rollbackSql, /dp\.packet_uid/,
      'hash authority must not expand observations by UID');
    assert.match(rollbackSql, /SELECT p\.packet_hash AS identity_key/,
      'hash authority resolves decryptions against the exact stored hash only');
  } finally {
    if (previous === undefined) delete process.env['PACKET_IDENTITY_READ_AUTHORITY'];
    else process.env['PACKET_IDENTITY_READ_AUTHORITY'] = previous;
  }
});

/** A disposable database carrying only the reader-shaped fixture schema. */
async function disposableFixture(t: TestContext): Promise<pg.Pool> {
  assert.ok(isLoopback(databaseUrl), SKIP_REASON);
  const fixture = await createDisposableDatabase(t, databaseUrl!, 'identity_readers');
  const pool = new pg.Pool({ connectionString: fixture.url, max: 2 });
  fixture.addCleanup(() => pool.end());
  await pool.query(FIXTURE_SCHEMA);
  return pool;
}

async function insertChannelPacket(
  pool: pg.Pool,
  row: { time: Date; hash: string; uid: Buffer; payload: Record<string, unknown> },
): Promise<void> {
  await pool.query(
    `INSERT INTO mesh.packets
       (time, packet_hash, packet_uid, rx_node_id, src_node_id, topic, topic_prefix, iata,
        packet_type, route_type, hop_count, rssi, snr, payload, advert_count, path_hashes,
        path_hash_size_bytes, network, transport_codes, region_scope, visibility_ok, is_private)
     VALUES ($1,$2,$3,$4,NULL,$5,'ukmesh','ZZZ',5,'flood',1,-90.5,6.25,$6::jsonb,NULL,'{}',1,
             'ukmesh','{}','#uk',TRUE,FALSE)`,
    [row.time, row.hash, row.uid, 'AB'.repeat(32), 'meshcore/ukmesh/AB/packets', JSON.stringify(row.payload)],
  );
}

/**
 * Channel history used to join `mesh.packet_decryptions` on the exact
 * observation hash. A message retained under two observation hashes can carry
 * its decryption on the older one, so the representative was either filtered
 * out of the candidate page (no summary to match) or returned with the
 * unhelpful ingest marker. The identity join must attribute the sibling's
 * decryption to the representative, in both the selection and detail queries.
 */
test('channel history resolves decryptions through the packet identity', {
  skip: isLoopback(databaseUrl) ? false : SKIP_REASON,
  timeout: 120_000,
}, async (t) => {
  const pool = await disposableFixture(t);
  const channel = 'test';
  const channelHashes = channelHashesForName(channel);
  assert.ok(channelHashes.length > 0, 'the fixture channel must have wire hashes');
  const now = Date.now();

  // A: the newest observation's summary is only the ingest marker. Pre-fix the
  // selection query cannot match it, because the decryption lives on the older
  // observation of the same UID.
  const uidA = uidBytes('d1');
  const newestA = hashes('e1');
  const siblingA = hashes('e2');
  await insertChannelPacket(pool, {
    time: new Date(now - 60_000), hash: newestA, uid: uidA, payload: { _summary: '[encrypted]' },
  });
  await insertChannelPacket(pool, {
    time: new Date(now - 120_000), hash: siblingA, uid: uidA, payload: { _summary: '[encrypted]' },
  });
  await pool.query(
    `INSERT INTO mesh.packet_decryptions (packet_hash, decrypted, summary) VALUES ($1, '{}'::jsonb, $2)`,
    [siblingA, '[test] sibling plaintext'],
  );

  // B: this row matches the channel hash directly, so pre-fix it is selected —
  // but its summary still comes from its own undecrypted observation.
  const uidB = uidBytes('d2');
  const newestB = hashes('e3');
  const siblingB = hashes('e4');
  await insertChannelPacket(pool, {
    time: new Date(now - 180_000), hash: newestB, uid: uidB,
    payload: { _summary: '[encrypted]', channelHash: channelHashes[0] },
  });
  await insertChannelPacket(pool, {
    time: new Date(now - 240_000), hash: siblingB, uid: uidB, payload: { _summary: '[encrypted]' },
  });
  await pool.query(
    `INSERT INTO mesh.packet_decryptions (packet_hash, decrypted, summary) VALUES ($1, '{}'::jsonb, $2)`,
    [siblingB, '[test] detail sibling'],
  );

  const readQuery: QueryFn = (sql, params) => pool.query(sql, params);
  const rows = await getChannelMessageHistory(channel, 10, 'ukmesh', undefined, readQuery);
  const byHash = new Map(rows.map((row) => [String(row.packet_hash), row]));

  assert.ok(byHash.has(newestA),
    'the representative must be selected through its sibling observation decryption');
  assert.equal(byHash.get(newestA)?.summary, '[test] sibling plaintext',
    'the selection and detail summary must be the identity decryption, not the ingest marker');
  assert.equal(byHash.get(newestB)?.summary, '[test] detail sibling',
    'a selected observation must show its sibling observation decryption');

  await pool.query('UPDATE mesh.packets SET is_private = TRUE WHERE packet_hash = $1', [siblingA]);
  await pool.query('UPDATE mesh.packets SET rx_node_id = $2 WHERE packet_hash = $1', [siblingB, 'BC'.repeat(32)]);
  const scoped = await getChannelMessageHistory(channel, 10, 'ukmesh', 'AB'.repeat(32), readQuery);
  const scopedByHash = new Map(scoped.map((row) => [String(row.packet_hash), row]));
  assert.equal(scopedByHash.has(newestA), false, 'private siblings cannot qualify a packet for history');
  assert.equal(scopedByHash.get(newestB)?.summary, '[test] [encrypted]',
    'a sibling outside the requested observer scope cannot enrich a selected packet');
});

/**
 * The RAM overlay resolves summaries for pending observations, which are often
 * not yet in `mesh.packets`. When an earlier observation of the same UID is
 * already stored and decrypted, the pending row must inherit that summary.
 */
test('RAM pending summaries resolve decryptions through the packet identity', {
  skip: isLoopback(databaseUrl) ? false : SKIP_REASON,
  timeout: 120_000,
}, async (t) => {
  const pool = await disposableFixture(t);
  const uid = uidBytes('f1');
  const pendingHash = hashes('f2');
  const siblingHash = hashes('f3');
  // The pending hash is already stored (a repeat observation of the same hash)
  // so its UID is known; the decryption sits on a different observation of the
  // same wire content.
  await insertChannelPacket(pool, {
    time: new Date(Date.now() - 20_000), hash: pendingHash, uid, payload: { _summary: '[encrypted]' },
  });
  await insertChannelPacket(pool, {
    time: new Date(Date.now() - 30_000), hash: siblingHash, uid, payload: { _summary: '[encrypted]' },
  });
  await pool.query(
    `INSERT INTO mesh.packet_decryptions (packet_hash, decrypted, summary) VALUES ($1, '{}'::jsonb, $2)`,
    [siblingHash, 'sibling summary'],
  );

  const readQuery: QueryFn = (sql, params) => pool.query(sql, params);
  const [summary] = await resolvePendingSummaries(
    [{ packet_hash: pendingHash, payload: { _summary: null } }],
    readQuery,
  ).then((map) => [map.get(pendingHash)]);
  assert.equal(summary, 'sibling summary',
    'a pending observation must inherit the decrypted sibling observation of its UID');

  // An ingest payload summary still outranks the durable decryption.
  const payloadWins = await resolvePendingSummaries(
    [{ packet_hash: pendingHash, payload: { _summary: 'payload summary' } }],
    readQuery,
  );
  assert.equal(payloadWins.get(pendingHash), 'payload summary');

  // A hash that is not stored yet keeps the exact fallback (no row => null).
  const absent = await resolvePendingSummaries(
    [{ packet_hash: hashes('f4'), payload: { _summary: null } }],
    readQuery,
  );
  assert.equal(absent.get(hashes('f4')), null);

  const unflushed = { packet_hash: hashes('f4'), packet_uid: uid.toString('hex'), payload: { _summary: null } };
  const matchedUid = await resolvePendingSummaries([unflushed], readQuery, { network: 'ukmesh', region: 'uk' });
  assert.equal(matchedUid.get(unflushed.packet_hash), 'sibling summary',
    'a pending packet can use its assigned UID without a committed hash');
  const otherRegion = await resolvePendingSummaries([unflushed], readQuery, { network: 'ukmesh', region: 'outside' });
  assert.equal(otherRegion.get(unflushed.packet_hash), null);

  // Rollback authority must not borrow a sibling observation's decryption.
  const previous = process.env['PACKET_IDENTITY_READ_AUTHORITY'];
  try {
    process.env['PACKET_IDENTITY_READ_AUTHORITY'] = 'hash';
    const rollback = await resolvePendingSummaries(
      [{ packet_hash: pendingHash, payload: { _summary: null } }],
      readQuery,
    );
    assert.equal(rollback.get(pendingHash), null);
  } finally {
    if (previous === undefined) delete process.env['PACKET_IDENTITY_READ_AUTHORITY'];
    else process.env['PACKET_IDENTITY_READ_AUTHORITY'] = previous;
  }
});
