import assert from 'node:assert/strict';
import test from 'node:test';
import type { QueryResultRow } from 'pg';
import { publicPacketPrivacySql } from '../api/utils/networkFilters.js';
import { DEFAULT_SPAM_MESSAGE_CONFIG as CFG } from './config.js';
import {
  getPublicIncident,
  getPublicIncidents,
  loadRecentMessages,
  type SpamSqlQuery,
} from './repository.js';

/**
 * The canonical privacy gate has four independent halves: the materialised
 * packet bits, the generation fence between the cache table and the live
 * visibility state, and both of those applied per member packet at read time.
 * Assert the fragments rather than the whole string so a regression in any
 * half fails loudly.
 */
function hasCanonicalPrivacyPredicate(sql: string): boolean {
  return /visibility_ok IS TRUE/.test(sql)
    && /is_private IS NOT TRUE/.test(sql)
    && /packet_visibility_materialization_state/.test(sql)
    && /public_visibility_state/.test(sql)
    && /visibility_generation\s*=\s*current_visibility\.generation/.test(sql);
}

const OBSERVED_AT = new Date('2026-10-23T12:00:00.000Z');
const PACKET_HASH = 'a'.repeat(64);

test('spam message and observer scans gate on the canonical packet privacy predicate', async () => {
  const captured: string[] = [];
  const runQuery: SpamSqlQuery = async <T extends QueryResultRow = QueryResultRow>(
    sql: string,
  ): Promise<{ rows: T[] }> => {
    captured.push(sql);
    if (/ranked_observers/.test(sql)) {
      return {
        rows: [{
          packet_hash: PACKET_HASH,
          observer_id: 'OBS1',
          lat: 53.4,
          lon: -2.2,
          min_hop: 1,
          best_rssi: -70,
          best_snr: 8,
        }] as unknown as T[],
      };
    }
    return {
      rows: [{
        packet_hash: PACKET_HASH,
        network: 'ukmesh',
        sender: 'PromoBot',
        message: 'buy cheap deals now at example.com',
        channel_hash: '80',
        summary: '[Public] test',
        claimed_ts: String(OBSERVED_AT.getTime()),
        observed_at: OBSERVED_AT,
      }] as unknown as T[],
    };
  };

  const records = await loadRecentMessages(
    CFG,
    { start: new Date(OBSERVED_AT.getTime() - 60_000), end: OBSERVED_AT },
    undefined,
    runQuery,
  );

  assert.equal(records.length, 1);
  assert.equal(captured.length, 2, 'message scan and observer scan must both run');
  for (const sql of captured) {
    assert.ok(hasCanonicalPrivacyPredicate(sql), `missing canonical privacy predicate in: ${sql}`);
    assert.ok(
      sql.includes(publicPacketPrivacySql('p')),
      'must embed the shared publicPacketPrivacySql helper, not a hand-rolled clause',
    );
  }
  // Keep main's additional content/name opt-out guards alongside the canonical
  // packet gate; neither display-name guard may replace that shared predicate.
  assert.match(captured[0]!, /COALESCE\(p\.payload->'decrypted'->>'sender', ''\) NOT LIKE '%🚫%'/);
  assert.match(captured[1]!, /n\.name IS NULL OR n\.name NOT LIKE '%🚫%'/);
});

test('public incident list re-applies packet privacy to stored incidents at read time', async () => {
  const captured: string[] = [];
  const runQuery: SpamSqlQuery = async <T extends QueryResultRow = QueryResultRow>(
    sql: string,
  ): Promise<{ rows: T[] }> => {
    captured.push(sql);
    return { rows: [] };
  };

  const rows = await getPublicIncidents({ status: 'active', limit: 5 }, runQuery);

  assert.deepEqual(rows, []);
  assert.equal(captured.length, 1);
  const sql = captured[0]!;
  assert.ok(hasCanonicalPrivacyPredicate(sql));
  assert.ok(sql.includes(publicPacketPrivacySql('p')));
  assert.match(sql, /FROM spam_message_incidents i/);
  assert.match(sql, /JOIN packets p/);
  assert.match(sql, /p\.packet_hash = m\.packet_hash/);
  assert.match(sql, /p\.network = m\.network/);
  assert.match(sql, /m\.incident_key = i\.incident_key/);
  assert.match(sql, /i\.status = \$1/);
  assert.doesNotMatch(sql, /NOT LIKE '%🚫%'/);
});

test('public incident detail re-applies packet privacy to the incident and its timeline', async () => {
  const captured: string[] = [];
  const runQuery: SpamSqlQuery = async <T extends QueryResultRow = QueryResultRow>(
    sql: string,
  ): Promise<{ rows: T[] }> => {
    captured.push(sql);
    if (/SELECT m\.observed_at/.test(sql)) return { rows: [] };
    return { rows: [{ public_json: { id: 'incident-1' } }] as unknown as T[] };
  };

  const incident = await getPublicIncident('incident-1', runQuery);

  assert.ok(incident);
  assert.equal(captured.length, 2, 'incident lookup and timeline lookup must both run');
  for (const sql of captured) {
    assert.ok(hasCanonicalPrivacyPredicate(sql), `missing canonical privacy predicate in: ${sql}`);
    assert.ok(sql.includes(publicPacketPrivacySql('p')));
    assert.match(sql, /packets p/);
  }
  assert.match(captured[0]!, /JOIN packets p/);
  assert.match(captured[1]!, /FROM spam_message_members m/);
  assert.match(captured[1]!, /p\.packet_hash = m\.packet_hash/);
});

// Negative control: the assertion helper must be able to fail. A query carrying
// only the retired sender/node name gate (no visibility bits, no generation
// fence) must not be accepted as privacy-safe.
test('canonical privacy assertion rejects the retired name-based gate', () => {
  const legacyGate = `AND COALESCE(p.payload->'decrypted'->>'sender', '') NOT LIKE '%🚫%'
    AND NOT EXISTS (
      SELECT 1 FROM nodes private_node
       WHERE private_node.name LIKE '%🚫%'
         AND private_node.node_id IN (p.rx_node_id, p.src_node_id)
    )`;
  assert.equal(hasCanonicalPrivacyPredicate(legacyGate), false);
  assert.doesNotMatch(legacyGate, /visibility_ok IS TRUE/);
  assert.doesNotMatch(legacyGate, /packet_visibility_materialization_state/);

  const unguarded = 'SELECT p.packet_hash FROM packets p WHERE p.packet_type = 5';
  assert.equal(hasCanonicalPrivacyPredicate(unguarded), false);
});
