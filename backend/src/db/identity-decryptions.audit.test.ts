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
 * The fixture builds only the forward-schema columns the reader touches: this
 * branch has no migration path that can bootstrap a fresh database (base.sql plus
 * the full ledger fails on the P1 schema moves), so a disposable database with a
 * narrow, reader-shaped schema is the smallest honest harness for the predicate.
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import type { QueryResultRow } from 'pg';
import { getRecentMessages, getRecentPackets } from '../repositories/packetReads.js';
import type { QueryFn } from './packetBatch.js';

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
  const admin = new pg.Pool({ connectionString: databaseUrl, max: 1 });
  const name = `meshcore_identity_audit_${process.pid}_${Date.now()}`;
  await admin.query(`CREATE DATABASE "${name}"`);
  const owned = new URL(databaseUrl!);
  owned.pathname = `/${name}`;
  const pool = new pg.Pool({ connectionString: owned.toString(), max: 2 });
  t.after(async () => {
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`);
    await admin.end();
  });

  await pool.query(FIXTURE_SCHEMA);

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
  await pool.query(insert, [new Date(now - 60_000), representative, uid, 'AB'.repeat(32),
    'meshcore/ukmesh/AB/packets', 'ukmesh', '{"direction":"rx"}']);
  await pool.query(insert, [new Date(now - 120_000), alias, uid, 'AB'.repeat(32),
    'meshcore/ukmesh/AB/packets', 'ukmesh', '{"direction":"rx"}']);
  await pool.query(insert, [new Date(now - 180_000), own, uidBytes('b2'), 'AB'.repeat(32),
    'meshcore/ukmesh/AB/packets', 'ukmesh', '{"direction":"rx"}']);
  await pool.query(insert, [new Date(now - 240_000), payloadWins, uidBytes('b3'), 'AB'.repeat(32),
    'meshcore/ukmesh/AB/packets', 'ukmesh', '{"direction":"rx","_summary":"payload summary"}']);
  await pool.query(insert, [new Date(now - 270_000), other, uidBytes('b4'), 'AB'.repeat(32),
    'meshcore/ukmesh/AB/packets', 'ukmesh', '{"direction":"rx"}']);

  // Only the alias observation of the shared UID was ever decrypted.
  const decrypt = `INSERT INTO mesh.packet_decryptions (packet_hash, decrypted, summary)
    VALUES ($1, '{}'::jsonb, $2)`;
  await pool.query(decrypt, [alias, 'sibling plaintext']);
  await pool.query(decrypt, [own, 'own plaintext']);
  await pool.query(decrypt, [payloadWins, 'decrypted but lower precedence']);

  const readQuery: QueryFn = (sql, params) => pool.query(sql, params);
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
