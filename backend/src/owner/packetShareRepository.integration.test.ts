import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import pg from 'pg';
import { createPacketShareRepository } from './packetShareRepository.js';
import type { DatabaseQueryFn } from '../db/contracts.js';
import type { LivePacket } from '../types/index.js';

const databaseUrl = process.env['PACKET_SHARE_TEST_DATABASE_URL'];
if (process.env['CI'] && !databaseUrl) {
  throw new Error('Packet-share SQL tests require PACKET_SHARE_TEST_DATABASE_URL in CI');
}

test('PostgreSQL enforces ScotMesh owner and observer scope for reads, rules and queue insertion', {
  skip: databaseUrl ? false : 'PACKET_SHARE_TEST_DATABASE_URL is not configured',
  timeout: 30_000,
}, async () => {
  // The repository and migrations are core-qualified after the P1 moves, so a
  // throwaway search_path cannot stand in for the post-move namespace. Provision
  // a dedicated database, create core, and apply the real migrations there.
  const connectionString = databaseUrl!;
  const admin = new pg.Client({ connectionString });
  await admin.connect();
  const database = `packet_share_test_${randomUUID().replaceAll('-', '')}`;
  try {
    await admin.query(`CREATE DATABASE ${database}`);
  } catch (error) {
    await admin.end();
    throw error;
  }
  const target = new URL(connectionString);
  target.pathname = `/${database}`;
  const pool = new pg.Pool({ connectionString: target.toString(), max: 1, options: '-c search_path=core,public' });
  const nodeId = 'A'.repeat(64);
  const packet: LivePacket = {
    id: 'event', packetHash: 'B'.repeat(16), rxNodeId: nodeId,
    topic: `meshcore/LHR/${nodeId}/packets`, ts: Date.now(), visibilityOk: true,
  };
  try {
    await pool.query('CREATE SCHEMA core');
    for (const migration of [
      '052_owner_packet_sharing.sql', '053_owner_packet_share_destination_metadata.sql',
      '054_owner_packet_share_default_off.sql', '058_packet_share_owner_scope_and_jwt.sql',
    ]) {
      await pool.query(await fs.readFile(new URL(`../db/migrations/${migration}`, import.meta.url), 'utf8'));
    }
    await pool.query(
      `INSERT INTO core.owner_packet_share_destinations (destination_id, display_name, enabled, allowed_owner_username, allowed_node_id)
       VALUES ('scotmesh', 'ScotMesh', TRUE, 'node1', $1), ('existing', 'Existing', TRUE, NULL, NULL)`, [nodeId],
    );
    const query: DatabaseQueryFn = async (sql, params) => pool.query(sql, params);
    // Forwarding now re-checks a live owner-node grant; supply the same authority
    // shape the production forwarder wires in.
    const repository = createPacketShareRepository(query, undefined, {
      isOwnerNodeGrantActive: async (ownerUsername, id) => ownerUsername === 'node1' && id === nodeId,
    });
    assert.deepEqual((await repository.listDestinations('node1', nodeId)).map((row) => row.destination_id), ['existing', 'scotmesh']);
    assert.deepEqual((await repository.listDestinations('node2', nodeId)).map((row) => row.destination_id), ['existing']);
    assert.deepEqual((await repository.listDestinations('node1', 'C'.repeat(64))).map((row) => row.destination_id), ['existing']);
    assert.deepEqual(await repository.replaceOwnerRules('node2', nodeId, ['scotmesh'], true), []);
    assert.deepEqual(await repository.replaceOwnerRules('node1', 'C'.repeat(64), ['scotmesh'], true), []);
    assert.deepEqual(await repository.replaceOwnerRules('node1', nodeId, ['scotmesh'], true), ['scotmesh']);
    // Simulate old rules created before scope enforcement; they must not become targets.
    await pool.query(
      `INSERT INTO core.owner_packet_share_rules (owner_username, node_id, destination_id, enabled)
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
    await pool.end();
    await admin.query(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
    await admin.end();
  }
});
