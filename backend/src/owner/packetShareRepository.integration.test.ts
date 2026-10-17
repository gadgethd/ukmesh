import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import pg from 'pg';
import { createPacketShareRepository } from './packetShareRepository.js';
import type { DatabaseQueryFn } from '../db/index.js';
import type { LivePacket } from '../types/index.js';

const databaseUrl = process.env['PACKET_SHARE_TEST_DATABASE_URL'];

test('PostgreSQL enforces ScotMesh owner and observer scope for reads, rules and queue insertion', {
  skip: !databaseUrl,
}, async () => {
  const schema = `packet_share_test_${randomUUID().replaceAll('-', '')}`;
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 1, options: `-c search_path=${schema}` });
  const nodeId = 'A'.repeat(64);
  const packet: LivePacket = {
    id: 'event', packetHash: 'B'.repeat(16), rxNodeId: nodeId,
    topic: `meshcore/LHR/${nodeId}/packets`, ts: Date.now(), visibilityOk: true,
  };
  try {
    await pool.query(`CREATE SCHEMA ${schema}`);
    for (const migration of [
      '052_owner_packet_sharing.sql', '053_owner_packet_share_destination_metadata.sql',
      '054_owner_packet_share_default_off.sql', '061_packet_share_owner_scope_and_jwt.sql',
    ]) {
      await pool.query(await fs.readFile(new URL(`../db/migrations/${migration}`, import.meta.url), 'utf8'));
    }
    await pool.query(
      `INSERT INTO owner_packet_share_destinations (destination_id, display_name, enabled, allowed_owner_username, allowed_node_id)
       VALUES ('scotmesh', 'ScotMesh', TRUE, 'node1', $1), ('existing', 'Existing', TRUE, NULL, NULL)`, [nodeId],
    );
    const query: DatabaseQueryFn = async (sql, params) => pool.query(sql, params);
    const repository = createPacketShareRepository(query);
    assert.deepEqual((await repository.listDestinations('node1', nodeId)).map((row) => row.destination_id), ['existing', 'scotmesh']);
    assert.deepEqual((await repository.listDestinations('node2', nodeId)).map((row) => row.destination_id), ['existing']);
    assert.deepEqual((await repository.listDestinations('node1', 'C'.repeat(64))).map((row) => row.destination_id), ['existing']);
    assert.deepEqual(await repository.replaceOwnerRules('node2', nodeId, ['scotmesh'], true), []);
    assert.deepEqual(await repository.replaceOwnerRules('node1', 'C'.repeat(64), ['scotmesh'], true), []);
    assert.deepEqual(await repository.replaceOwnerRules('node1', nodeId, ['scotmesh'], true), ['scotmesh']);
    // Simulate old rules created before scope enforcement; they must not become targets.
    await pool.query(
      `INSERT INTO owner_packet_share_rules (owner_username, node_id, destination_id, enabled)
       VALUES ('node2', $1, 'scotmesh', TRUE), ('node1', $2, 'scotmesh', TRUE)`, [nodeId, 'C'.repeat(64)],
    );
    assert.deepEqual(await repository.listOwnerRules('node2', nodeId), []);
    assert.deepEqual((await repository.activeTargets(nodeId)).map((row) => row.owner_username), ['node1']);
    assert.deepEqual(await repository.activeTargets('C'.repeat(64)), []);
    for (const [ownerUsername, observer, expected] of [
      ['node1', nodeId, true], ['node2', nodeId, false], ['node1', 'C'.repeat(64), false],
    ] as const) {
      assert.equal(await repository.enqueueDelivery({
        ownerUsername, nodeId: observer, destinationId: 'scotmesh',
        eventKey: `${ownerUsername}:${observer}`, packet: { ...packet, rxNodeId: observer },
        destinationTopic: `meshcore/MME/${observer}/packets`, payload: { raw: '0102' },
      }), expected);
    }
  } finally {
    await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
    await pool.end();
  }
});
