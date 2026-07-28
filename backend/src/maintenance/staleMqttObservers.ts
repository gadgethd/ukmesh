import crypto from 'node:crypto';
import { pool } from '../db/index.js';

type QueryResultLike<Row = Record<string, unknown>> = {
  rows: Row[];
  rowCount: number | null;
};

type CleanupClient = {
  query<Row = Record<string, unknown>>(text: string, values?: unknown[]): Promise<QueryResultLike<Row>>;
  release(): void;
};

type CleanupPool = {
  connect(): Promise<CleanupClient>;
};

export type StaleMqttObserverCleanupResult = {
  batchId: string | null;
  candidates: number;
  nodes: number;
  observerSightings: number;
  networkSightings: number;
};

type CleanupOptions = {
  cleanupPool?: CleanupPool;
  thresholdDays?: number;
  batchId?: string;
};

function boundedThresholdDays(value: number | undefined): number {
  if (!Number.isFinite(value)) return 30;
  return Math.min(365, Math.max(30, Math.trunc(value!)));
}

/**
 * Archive and remove repeater-class nodes whose own observer MQTT feed has
 * been silent for the configured period. Authentication and packet history
 * live in separate tables and are deliberately outside this transaction.
 */
export async function cleanupStaleMqttObservers(
  options: CleanupOptions = {},
): Promise<StaleMqttObserverCleanupResult> {
  const cleanupPool = (options.cleanupPool ?? pool) as unknown as CleanupPool;
  const thresholdDays = boundedThresholdDays(options.thresholdDays);
  const client = await cleanupPool.connect();

  try {
    await client.query('BEGIN');
    await client.query(`SELECT pg_advisory_xact_lock(hashtext('stale-mqtt-observer-cleanup'))`);

    // Lock the node row used by MQTT's atomic observer upsert. If an observer
    // returns during cleanup, it either becomes fresh before this selection or
    // waits and recreates itself immediately after the deletion commits.
    const candidates = await client.query<{ node_id: string }>(
      `SELECT node_id
         FROM nodes
        WHERE last_mqtt_observer_seen_at < NOW() - ($1 * INTERVAL '1 day')
          AND network IS DISTINCT FROM 'test'
          AND (role IS NULL OR role = 2)
        ORDER BY node_id
        FOR UPDATE`,
      [thresholdDays],
    );
    const nodeIds = candidates.rows.map((row) => row.node_id);
    if (nodeIds.length === 0) {
      await client.query('COMMIT');
      return {
        batchId: null,
        candidates: 0,
        nodes: 0,
        observerSightings: 0,
        networkSightings: 0,
      };
    }

    const batchId = options.batchId ?? `stale-mqtt-observer-${new Date().toISOString()}-${crypto.randomUUID()}`;
    const reason = `MQTT observer silent for at least ${thresholdDays} days`;

    await client.query(
      `INSERT INTO maintenance_removed_records (batch_id, source_table, record_data, reason)
       SELECT $1, 'nodes', to_jsonb(n), $2
         FROM nodes n
        WHERE n.node_id = ANY($3::text[])`,
      [batchId, reason, nodeIds],
    );
    await client.query(
      `INSERT INTO maintenance_removed_records (batch_id, source_table, record_data, reason)
       SELECT $1, 'observer_region_observer_sightings', to_jsonb(s), $2
         FROM observer_region_observer_sightings s
        WHERE s.rx_node_id = ANY($3::text[])`,
      [batchId, reason, nodeIds],
    );
    await client.query(
      `INSERT INTO maintenance_removed_records (batch_id, source_table, record_data, reason)
       SELECT $1, 'node_network_sightings', to_jsonb(s), $2
         FROM node_network_sightings s
        WHERE s.node_id = ANY($3::text[])`,
      [batchId, reason, nodeIds],
    );

    const observerSightings = await client.query(
      `DELETE FROM observer_region_observer_sightings
        WHERE rx_node_id = ANY($1::text[])
        RETURNING 1`,
      [nodeIds],
    );
    const networkSightings = await client.query(
      `DELETE FROM node_network_sightings
        WHERE node_id = ANY($1::text[])
        RETURNING 1`,
      [nodeIds],
    );
    const nodes = await client.query(
      `DELETE FROM nodes
        WHERE node_id = ANY($1::text[])
        RETURNING 1`,
      [nodeIds],
    );

    await client.query('COMMIT');
    return {
      batchId,
      candidates: nodeIds.length,
      nodes: nodes.rowCount ?? 0,
      observerSightings: observerSightings.rowCount ?? 0,
      networkSightings: networkSightings.rowCount ?? 0,
    };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
