import type { DatabaseQueryFn } from '../db/index.js';
import type { LivePacket } from '../types/index.js';

export type PublicFeedQueryFn = DatabaseQueryFn;
export type PublicFeedTransactionFn = <T>(work: (query: PublicFeedQueryFn) => Promise<T>) => Promise<T>;

export type PublicFeedAuthorizationSnapshot = {
  mqttUsername: string;
  isActive: boolean;
  nodeIds: string[];
};

export type PublicFeedDeliveryRow = {
  id: string;
  node_id: string;
  event_key: string;
  packet_hash: string;
  source_topic: string;
  destination_topic: string;
  payload: Record<string, unknown>;
  attempts: number;
};

const NORMALIZED_NODE_ID_RE = /^[0-9A-F]{64}$/;

function normalizedNodeIds(nodeIds: string[]): string[] {
  return Array.from(new Set(
    nodeIds
      .map((nodeId) => nodeId.trim().toUpperCase())
      .filter((nodeId) => NORMALIZED_NODE_ID_RE.test(nodeId)),
  ));
}

/**
 * A public-feed reader is granted only while its opted-in account has produced
 * at least one public packet observation in the rolling 24-hour window. This
 * set is consumed by the broker ACL reconciler; it is deliberately derived
 * from the materialised verified owner mapping rather than owner credentials.
 */
export async function getActivePublicFeedReaderUsernames(
  query: PublicFeedQueryFn,
): Promise<Set<string>> {
  const result = await query<{ owner_username: string }>(
    `SELECT DISTINCT feed_node.owner_username
       FROM owner_public_feed_nodes feed_node
       JOIN owner_public_feed_settings settings
         ON settings.owner_username = feed_node.owner_username
        AND settings.enabled = TRUE
      WHERE EXISTS (
        SELECT 1
          FROM packets packet
         WHERE packet.rx_node_id = feed_node.node_id
           AND packet.time > NOW() - INTERVAL '24 hours'
           AND packet.network IS DISTINCT FROM 'test'
           AND packet.visibility_ok IS TRUE
           AND packet.is_private IS NOT TRUE
      )`,
  );
  return new Set(result.rows.map((row) => row.owner_username));
}

/**
 * Return the observer IDs that are both opted in and currently contributing
 * eligible public packets. The forwarder uses this set to decide which
 * observers may be republished into the curated public namespace.
 */
export async function getActivePublicFeedNodeIds(
  query: PublicFeedQueryFn,
): Promise<string[]> {
  const result = await query<{ node_id: string }>(
    `SELECT DISTINCT feed_node.node_id
       FROM owner_public_feed_nodes feed_node
       JOIN owner_public_feed_settings settings
         ON settings.owner_username = feed_node.owner_username
        AND settings.enabled = TRUE
      WHERE EXISTS (
        SELECT 1
          FROM packets packet
         WHERE packet.rx_node_id = feed_node.node_id
           AND packet.time > NOW() - INTERVAL '24 hours'
           AND packet.network IS DISTINCT FROM 'test'
           AND packet.visibility_ok IS TRUE
           AND packet.is_private IS NOT TRUE
      )
      ORDER BY feed_node.node_id`,
  );
  return normalizedNodeIds(result.rows.map((row) => row.node_id)).sort();
}

export function createPublicFeedRepository(
  query: PublicFeedQueryFn,
  transaction?: PublicFeedTransactionFn,
) {
  async function accountStatus(ownerUsername: string, nodeIds: string[]): Promise<{
    enabled: boolean;
    nodeCount: number;
    eligible: boolean;
    recentPacketCount: number;
    lastForwardedAt: string | null;
  }> {
    const activeNodeIds = normalizedNodeIds(nodeIds);
    const result = await query<{
      enabled: boolean | null;
      recent_packet_count: number | string | null;
      last_forwarded_at: Date | string | null;
    }>(
      `WITH recent_activity AS (
        SELECT COUNT(*)::int AS recent_packet_count
          FROM packets packet
         WHERE packet.rx_node_id = ANY($2::text[])
           AND packet.time > NOW() - INTERVAL '24 hours'
           AND packet.network IS DISTINCT FROM 'test'
           AND packet.visibility_ok IS TRUE
           AND packet.is_private IS NOT TRUE
      )
      SELECT COALESCE(settings.enabled, FALSE) AS enabled,
             recent_activity.recent_packet_count,
             MAX(delivery.delivered_at) AS last_forwarded_at
         FROM (SELECT $1::text AS owner_username) requested
         CROSS JOIN recent_activity
         LEFT JOIN owner_public_feed_settings settings
           ON settings.owner_username = requested.owner_username
         LEFT JOIN owner_public_feed_nodes feed_node
           ON feed_node.owner_username = requested.owner_username
         LEFT JOIN public_feed_deliveries delivery
           ON delivery.node_id = feed_node.node_id
          AND delivery.status = 'succeeded'
          AND delivery.delivered_at IS NOT NULL
        GROUP BY settings.enabled, recent_activity.recent_packet_count`,
      [ownerUsername, activeNodeIds],
    );
    const row = result.rows[0];
    const recentPacketCount = Number(row?.recent_packet_count ?? 0);
    return {
      enabled: row?.enabled === true,
      nodeCount: nodeIds.length,
      eligible: recentPacketCount > 0,
      recentPacketCount: Number.isFinite(recentPacketCount) ? recentPacketCount : 0,
      lastForwardedAt: row?.last_forwarded_at
        ? new Date(row.last_forwarded_at).toISOString()
        : null,
    };
  }

  async function activeNodeIds(): Promise<string[]> {
    return getActivePublicFeedNodeIds(query);
  }

  async function setEnabled(
    ownerUsername: string,
    nodeIds: string[],
    enabled: boolean,
  ): Promise<void> {
    const update = async (runQuery: PublicFeedQueryFn): Promise<void> => {
      await runQuery(
        `INSERT INTO owner_public_feed_settings (owner_username, enabled, updated_at)
         VALUES ($1, $2, NOW())
         ON CONFLICT (owner_username) DO UPDATE SET
           enabled = EXCLUDED.enabled,
           updated_at = NOW()`,
        [ownerUsername, enabled],
      );
      await runQuery(
        `DELETE FROM owner_public_feed_nodes
          WHERE owner_username = $1`,
        [ownerUsername],
      );
      if (enabled && nodeIds.length > 0) {
        await runQuery(
          `INSERT INTO owner_public_feed_nodes (owner_username, node_id)
           SELECT $1, node_id
             FROM unnest($2::text[]) AS selected(node_id)
           ON CONFLICT (owner_username, node_id) DO NOTHING`,
          [ownerUsername, nodeIds],
        );
      }
      if (!enabled) {
        await runQuery(
          `UPDATE public_feed_deliveries delivery
              SET status = 'dead_lettered',
                  last_error = 'public feed disabled',
                  claim_token = NULL,
                  claim_expires_at = NULL,
                  updated_at = NOW()
            WHERE delivery.status IN ('pending', 'failed')
              AND NOT EXISTS (
                SELECT 1
                  FROM owner_public_feed_nodes active_node
                 WHERE active_node.node_id = delivery.node_id
              )`,
        );
      }
    };
    if (transaction) {
      await transaction(update);
      return;
    }
    await update(query);
  }

  /**
   * Rebuild the materialised owner-to-node mapping from the verified owner
   * database. The snapshot is supplied by the owner-auth module so this
   * analytics database never becomes an independent source of ownership.
   */
  async function syncOwnership(snapshot: PublicFeedAuthorizationSnapshot[]): Promise<void> {
    const pairSet = new Set<string>();
    const usernames: string[] = [];
    const nodeIds: string[] = [];
    for (const account of snapshot) {
      if (!account.isActive) continue;
      const username = account.mqttUsername.trim();
      if (!username) continue;
      for (const rawNodeId of account.nodeIds) {
        const nodeId = rawNodeId.trim().toUpperCase();
        const pair = `${username}\u0000${nodeId}`;
        if (!/^[0-9A-F]{64}$/.test(nodeId) || pairSet.has(pair)) continue;
        pairSet.add(pair);
        usernames.push(username);
        nodeIds.push(nodeId);
      }
    }

    const update = async (runQuery: PublicFeedQueryFn): Promise<void> => {
      await runQuery(
        `DELETE FROM owner_public_feed_nodes feed_node
          WHERE NOT EXISTS (
            SELECT 1
              FROM unnest($1::text[], $2::text[]) AS requested(owner_username, node_id)
             WHERE requested.owner_username = feed_node.owner_username
               AND requested.node_id = feed_node.node_id
          )
             OR NOT EXISTS (
            SELECT 1
              FROM owner_public_feed_settings settings
             WHERE settings.owner_username = feed_node.owner_username
               AND settings.enabled = TRUE
          )`,
        [usernames, nodeIds],
      );
      if (usernames.length > 0) {
        await runQuery(
          `INSERT INTO owner_public_feed_nodes (owner_username, node_id)
           SELECT requested.owner_username, requested.node_id
             FROM unnest($1::text[], $2::text[]) AS requested(owner_username, node_id)
             JOIN owner_public_feed_settings settings
               ON settings.owner_username = requested.owner_username
              AND settings.enabled = TRUE
           ON CONFLICT (owner_username, node_id) DO NOTHING`,
          [usernames, nodeIds],
        );
      }
      await runQuery(
        `UPDATE public_feed_deliveries delivery
            SET status = 'dead_lettered',
                last_error = 'public feed ownership changed',
                claim_token = NULL,
                claim_expires_at = NULL,
                updated_at = NOW()
          WHERE delivery.status IN ('pending', 'failed')
            AND NOT EXISTS (
              SELECT 1
                FROM owner_public_feed_nodes active_node
               WHERE active_node.node_id = delivery.node_id
            )`,
      );
    };
    if (transaction) {
      await transaction(update);
      return;
    }
    await update(query);
  }

  async function hasActiveNode(nodeId: string): Promise<boolean> {
    const result = await query(
      `SELECT 1
         FROM owner_public_feed_nodes feed_node
         JOIN owner_public_feed_settings settings
           ON settings.owner_username = feed_node.owner_username
        WHERE feed_node.node_id = $1
          AND settings.enabled = TRUE
        LIMIT 1`,
      [nodeId],
    );
    return result.rows.length > 0;
  }

  async function enqueueDelivery(input: {
    nodeId: string;
    eventKey: string;
    packet: LivePacket;
    destinationTopic: string;
    payload: Record<string, unknown>;
  }): Promise<boolean> {
    const result = await query(
      `INSERT INTO public_feed_deliveries (
         node_id, event_key, packet_hash, source_topic, destination_topic, payload
       )
       SELECT $1, $2, $3, $4, $5, $6::jsonb
        WHERE EXISTS (
          SELECT 1
            FROM owner_public_feed_nodes feed_node
            JOIN owner_public_feed_settings settings
              ON settings.owner_username = feed_node.owner_username
           WHERE feed_node.node_id = $1
             AND settings.enabled = TRUE
        )
       ON CONFLICT (event_key) DO NOTHING
       RETURNING id`,
      [
        input.nodeId,
        input.eventKey,
        input.packet.packetHash,
        input.packet.topic,
        input.destinationTopic,
        JSON.stringify(input.payload),
      ],
    );
    return result.rows.length > 0;
  }

  async function reclaimExpiredClaims(now = new Date()): Promise<number> {
    const result = await query<{ count: string }>(
      `WITH reclaimed AS (
         UPDATE public_feed_deliveries
            SET status = CASE WHEN attempts >= 5 THEN 'dead_lettered' ELSE 'failed' END,
                next_attempt_at = NOW(),
                claim_token = NULL,
                claim_expires_at = NULL,
                updated_at = NOW()
          WHERE status = 'delivering'
            AND claim_expires_at < $1
          RETURNING id
       ) SELECT COUNT(*)::text AS count FROM reclaimed`,
      [now],
    );
    return Number(result.rows[0]?.count ?? 0);
  }

  async function discardInactiveDeliveries(): Promise<number> {
    const result = await query<{ id: string }>(
      `UPDATE public_feed_deliveries delivery
          SET status = 'dead_lettered',
              last_error = 'public feed is no longer enabled for this node',
              claim_token = NULL,
              claim_expires_at = NULL,
              updated_at = NOW()
        WHERE delivery.status IN ('pending', 'failed')
          AND NOT EXISTS (
            SELECT 1
              FROM owner_public_feed_nodes feed_node
              JOIN owner_public_feed_settings settings
                ON settings.owner_username = feed_node.owner_username
             WHERE feed_node.node_id = delivery.node_id
               AND settings.enabled = TRUE
          )
        RETURNING delivery.id::text`,
    );
    return result.rows.length;
  }

  async function claimDue(limit: number, claimToken: string): Promise<PublicFeedDeliveryRow[]> {
    const result = await query<PublicFeedDeliveryRow>(
      `WITH due AS (
         SELECT delivery.id
           FROM public_feed_deliveries delivery
          WHERE delivery.status IN ('pending', 'failed')
            AND delivery.attempts < 5
            AND delivery.next_attempt_at <= NOW()
            AND EXISTS (
              SELECT 1
                FROM owner_public_feed_nodes feed_node
                JOIN owner_public_feed_settings settings
                  ON settings.owner_username = feed_node.owner_username
               WHERE feed_node.node_id = delivery.node_id
                 AND settings.enabled = TRUE
            )
          ORDER BY delivery.next_attempt_at, delivery.id
          FOR UPDATE SKIP LOCKED
          LIMIT $1
       )
       UPDATE public_feed_deliveries delivery
          SET status = 'delivering',
              attempts = delivery.attempts + 1,
              last_attempt_at = NOW(),
              claim_token = $2,
              claim_expires_at = NOW() + INTERVAL '30 seconds',
              updated_at = NOW()
         FROM due
        WHERE delivery.id = due.id
       RETURNING delivery.id::text, delivery.node_id, delivery.event_key,
                 delivery.packet_hash, delivery.source_topic,
                 delivery.destination_topic, delivery.payload, delivery.attempts`,
      [limit, claimToken],
    );
    return result.rows;
  }

  async function markSucceeded(id: string, claimToken: string): Promise<void> {
    await query(
      `UPDATE public_feed_deliveries
          SET status = 'succeeded', delivered_at = NOW(), last_error = NULL,
              claim_token = NULL, claim_expires_at = NULL, updated_at = NOW()
        WHERE id = $1 AND claim_token = $2`,
      [id, claimToken],
    );
  }

  async function markFailed(id: string, claimToken: string, attempts: number, error: string): Promise<void> {
    const terminal = attempts >= 5;
    const delaySeconds = Math.min(300, 2 ** Math.max(0, attempts - 1));
    await query(
      `UPDATE public_feed_deliveries
          SET status = $3,
              next_attempt_at = NOW() + ($4::text || ' seconds')::interval,
              last_error = $5, claim_token = NULL, claim_expires_at = NULL,
              updated_at = NOW()
        WHERE id = $1 AND claim_token = $2`,
      [id, claimToken, terminal ? 'dead_lettered' : 'failed', String(delaySeconds), error.slice(0, 500)],
    );
  }

  async function markDiscarded(id: string, claimToken: string, error: string): Promise<void> {
    await query(
      `UPDATE public_feed_deliveries
          SET status = 'dead_lettered', last_error = $3,
              claim_token = NULL, claim_expires_at = NULL, updated_at = NOW()
        WHERE id = $1 AND claim_token = $2`,
      [id, claimToken, error.slice(0, 500)],
    );
  }

  async function queueStats(): Promise<{ count: number; oldestAgeSeconds: number }> {
    const result = await query<{ count: string; age: string | null }>(`SELECT COUNT(*)::text AS count,
      EXTRACT(EPOCH FROM NOW() - MIN(created_at))::text AS age FROM public_feed_deliveries
      WHERE status IN ('pending', 'failed', 'delivering')`);
    return { count: Number(result.rows[0]?.count ?? 0), oldestAgeSeconds: Math.max(0, Number(result.rows[0]?.age ?? 0)) };
  }

  return {
    queueStats,
    accountStatus,
    activeNodeIds,
    setEnabled,
    syncOwnership,
    hasActiveNode,
    enqueueDelivery,
    reclaimExpiredClaims,
    discardInactiveDeliveries,
    claimDue,
    markSucceeded,
    markFailed,
    markDiscarded,
  };
}

export type PublicFeedRepository = ReturnType<typeof createPublicFeedRepository>;
