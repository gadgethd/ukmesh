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

export async function cleanupStaleMqttObservers(
  options: CleanupOptions = {},
): Promise<StaleMqttObserverCleanupResult> {
  return cleanupNodeRecords('mqtt-observer', options);
}

/** Archive inactive nodes of every role, including nodes never bridged to MQTT. */
export async function cleanupInactiveNodes(
  options: CleanupOptions = {},
): Promise<StaleMqttObserverCleanupResult> {
  return cleanupNodeRecords('inactive-node', options);
}

/** Authentication and packet history are deliberately outside this transaction. */
async function cleanupNodeRecords(
  kind: 'mqtt-observer' | 'inactive-node',
  options: CleanupOptions,
): Promise<StaleMqttObserverCleanupResult> {
  const cleanupPool = (options.cleanupPool ?? pool) as unknown as CleanupPool;
  const thresholdDays = boundedThresholdDays(options.thresholdDays);
  const client = await cleanupPool.connect();

  try {
    await client.query('BEGIN');
    await client.query(`SELECT pg_advisory_xact_lock(hashtext('stale-mqtt-observer-cleanup'))`);

    // Lock the node row used by MQTT's atomic upsert. If a node
    // returns during cleanup, it either becomes fresh before this selection or
    // waits and recreates itself immediately after the deletion commits.
    const staleCondition = kind === 'mqtt-observer'
      ? `last_mqtt_observer_seen_at < NOW() - ($1 * INTERVAL '1 day')
         AND (role IS NULL OR role = 2)`
      : `GREATEST(last_seen, last_mqtt_observer_seen_at, last_path_evidence_at,
                  last_predicted_online_at, created_at) < NOW() - ($1 * INTERVAL '1 day')
         AND NOT EXISTS (
           SELECT 1 FROM node_network_sightings s
            WHERE s.node_id = n.node_id
              AND s.last_seen_at >= NOW() - ($1 * INTERVAL '1 day')
         )
         AND NOT EXISTS (
           SELECT 1 FROM observer_region_observer_sightings s
            WHERE s.rx_node_id = n.node_id
              AND s.last_seen >= NOW() - ($1 * INTERVAL '1 day')
         )`;
    const candidates = await client.query<{ node_id: string }>(
      `SELECT node_id
         FROM nodes n
        WHERE ${staleCondition}
          AND network IS DISTINCT FROM 'test'
          -- Node deletion cascades its privacy prefixes and the existing
          -- node trigger then republishes previously private packet history.
          -- Age alone must never lift that privacy decision. Keep the identity
          -- until a separate privacy-preserving tombstone policy is available.
          AND COALESCE(n.name, '') NOT LIKE '%🚫%'
          AND NOT EXISTS (
            SELECT 1 FROM private_node_prefixes p WHERE p.node_id = n.node_id
          )
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

    const batchId = options.batchId ?? `stale-${kind}-${new Date().toISOString()}-${crypto.randomUUID()}`;
    const reason = kind === 'mqtt-observer'
      ? `MQTT observer silent for at least ${thresholdDays} days`
      : `Node has no observed activity for at least ${thresholdDays} days`;

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

    // The prefix FK's cascading DELETE fires its statement trigger even when
    // these public nodes have no prefixes. At trigger depth 1 it advances the
    // public generation without re-fencing stored packets. Serialize that
    // change, and preserve only a fence that was already current. The locked
    // candidates cannot gain a privacy marker/prefix before deletion.
    const visibilityState = await client.query<{ materialization_current: boolean }>(
      `SELECT visibility.generation = materialized.visibility_generation AS materialization_current
         FROM public_visibility_state visibility
         LEFT JOIN packet_visibility_materialization_state materialized
           ON materialized.singleton = visibility.singleton
        WHERE visibility.singleton = TRUE
        FOR UPDATE OF visibility`,
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

    if (visibilityState.rows[0]?.materialization_current === true) {
      await client.query(
        `UPDATE packet_visibility_materialization_state materialized
            SET visibility_generation = visibility.generation, updated_at = NOW()
           FROM public_visibility_state visibility
          WHERE materialized.singleton = TRUE AND visibility.singleton = TRUE
            AND materialized.visibility_generation IS DISTINCT FROM visibility.generation`,
      );
    }

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
