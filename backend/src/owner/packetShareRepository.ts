import type { DatabaseQueryFn } from '../db/index.js';
import type { LivePacket } from '../types/index.js';
import type { PacketShareDestinationRow, PacketShareTarget } from './packetShareConfig.js';

export type PacketShareQueryFn = DatabaseQueryFn;
export type PacketShareTransactionFn = <T>(work: (query: PacketShareQueryFn) => Promise<T>) => Promise<T>;

export type PacketShareDeliveryRow = {
  id: string;
  owner_username: string;
  node_id: string;
  destination_id: string;
  event_key: string;
  packet_hash: string;
  source_topic: string;
  destination_topic: string;
  payload: Record<string, unknown>;
  attempts: number;
};

export function createPacketShareRepository(
  query: PacketShareQueryFn,
  transaction?: PacketShareTransactionFn,
) {
  async function listDestinations(): Promise<PacketShareDestinationRow[]> {
    const result = await query<PacketShareDestinationRow>(
      `SELECT destination_id, display_name, website_url, description,
              broker_url, topic_template,
              broker_username, broker_password_ciphertext, enabled
         FROM owner_packet_share_destinations
        ORDER BY display_name, destination_id`,
    );
    return result.rows;
  }

  async function getDestination(destinationId: string): Promise<PacketShareDestinationRow | null> {
    const result = await query<PacketShareDestinationRow>(
      `SELECT destination_id, display_name, website_url, description,
              broker_url, topic_template,
              broker_username, broker_password_ciphertext, enabled
         FROM owner_packet_share_destinations
        WHERE destination_id = $1`,
      [destinationId],
    );
    return result.rows[0] ?? null;
  }

  async function listOwnerRules(ownerUsername: string, nodeId: string): Promise<string[]> {
    const result = await query<{ destination_id: string }>(
      `SELECT destination_id
         FROM owner_packet_share_rules
        WHERE owner_username = $1 AND node_id = $2 AND enabled = TRUE
        ORDER BY destination_id`,
      [ownerUsername, nodeId],
    );
    return result.rows.map((row) => row.destination_id);
  }

  async function listLastForwardedAt(ownerUsername: string, nodeId: string): Promise<Map<string, string>> {
    const result = await query<{ destination_id: string; last_forwarded_at: Date | string }>(
      `SELECT destination_id, MAX(delivered_at) AS last_forwarded_at
         FROM owner_packet_share_deliveries
        WHERE owner_username = $1
          AND node_id = $2
          AND status = 'succeeded'
          AND delivered_at IS NOT NULL
        GROUP BY destination_id`,
      [ownerUsername, nodeId],
    );
    return new Map(result.rows.map((row) => [row.destination_id, new Date(row.last_forwarded_at).toISOString()]));
  }

  async function activeTargets(nodeId: string): Promise<PacketShareTarget[]> {
    const result = await query<PacketShareTarget>(
      `SELECT rules.owner_username, rules.node_id,
              destination.destination_id, destination.display_name,
              destination.website_url, destination.description,
              destination.broker_url, destination.topic_template,
              destination.broker_username, destination.broker_password_ciphertext,
              destination.enabled
         FROM owner_packet_share_rules rules
         JOIN owner_packet_share_destinations destination
           ON destination.destination_id = rules.destination_id
        WHERE rules.node_id = $1
          AND rules.enabled = TRUE
          AND destination.enabled = TRUE
        ORDER BY destination.destination_id`,
      [nodeId],
    );
    return result.rows;
  }

  async function replaceOwnerRules(
    ownerUsername: string,
    nodeId: string,
    destinationIds: string[],
    enabled: boolean,
  ): Promise<string[]> {
    const update = async (runQuery: PacketShareQueryFn): Promise<string[]> => {
      await runQuery(
        `DELETE FROM owner_packet_share_rules
          WHERE owner_username = $1 AND node_id = $2`,
        [ownerUsername, nodeId],
      );
      await runQuery(
        `UPDATE owner_packet_share_deliveries
            SET status = 'dead_lettered',
                last_error = 'sharing rule disabled',
                claim_token = NULL,
                claim_expires_at = NULL,
                updated_at = NOW()
          WHERE owner_username = $1
            AND node_id = $2
            AND status IN ('pending', 'failed', 'delivering')
            AND ($4::boolean = FALSE OR destination_id <> ALL($3::text[]))`,
        [ownerUsername, nodeId, destinationIds, enabled],
      );
      if (!enabled || destinationIds.length < 1) return [];
      const result = await runQuery<{ destination_id: string }>(
        `INSERT INTO owner_packet_share_rules (owner_username, node_id, destination_id, enabled)
         SELECT $1, $2, destination_id, TRUE
           FROM owner_packet_share_destinations
          WHERE destination_id = ANY($3::text[])
            AND enabled = TRUE
         RETURNING destination_id`,
        [ownerUsername, nodeId, destinationIds],
      );
      return result.rows.map((row) => row.destination_id).sort();
    };
    if (transaction) {
      return transaction(update);
    }
    return update(query);
  }

  async function enqueueDelivery(input: {
    ownerUsername: string;
    nodeId: string;
    destinationId: string;
    eventKey: string;
    packet: LivePacket;
    destinationTopic: string;
    payload: Record<string, unknown>;
  }): Promise<boolean> {
    const result = await query(
      `INSERT INTO owner_packet_share_deliveries (
         owner_username, node_id, destination_id, event_key,
         packet_hash, source_topic, destination_topic, payload
       )
       SELECT $1, $2, $3, $4, $5, $6, $7, $8::jsonb
        WHERE EXISTS (
          SELECT 1
            FROM owner_packet_share_rules rules
            JOIN owner_packet_share_destinations destination
              ON destination.destination_id = rules.destination_id
           WHERE rules.owner_username = $1
             AND rules.node_id = $2
             AND rules.destination_id = $3
             AND rules.enabled = TRUE
             AND destination.enabled = TRUE
        )
       ON CONFLICT (event_key) DO NOTHING
       RETURNING id`,
      [
        input.ownerUsername,
        input.nodeId,
        input.destinationId,
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
         UPDATE owner_packet_share_deliveries
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

  async function claimDue(limit: number, claimToken: string): Promise<PacketShareDeliveryRow[]> {
    const result = await query<PacketShareDeliveryRow>(
      `WITH due AS (
         SELECT id
           FROM owner_packet_share_deliveries
          WHERE status IN ('pending', 'failed')
            AND attempts < 5
            AND next_attempt_at <= NOW()
          ORDER BY next_attempt_at, id
          FOR UPDATE SKIP LOCKED
          LIMIT $1
       )
       UPDATE owner_packet_share_deliveries delivery
          SET status = 'delivering',
              attempts = delivery.attempts + 1,
              last_attempt_at = NOW(),
              claim_token = $2,
              claim_expires_at = NOW() + INTERVAL '30 seconds',
              updated_at = NOW()
         FROM due
        WHERE delivery.id = due.id
       RETURNING delivery.id::text, delivery.owner_username, delivery.node_id,
                 delivery.destination_id, delivery.event_key, delivery.packet_hash,
                 delivery.source_topic, delivery.destination_topic,
                 delivery.payload, delivery.attempts`,
      [limit, claimToken],
    );
    return result.rows;
  }

  async function markSucceeded(id: string, claimToken: string): Promise<void> {
    await query(
      `UPDATE owner_packet_share_deliveries
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
      `UPDATE owner_packet_share_deliveries
          SET status = $3,
              next_attempt_at = NOW() + ($4::text || ' seconds')::interval,
              last_error = $5, claim_token = NULL, claim_expires_at = NULL,
              updated_at = NOW()
        WHERE id = $1 AND claim_token = $2`,
      [id, claimToken, terminal ? 'dead_lettered' : 'failed', String(delaySeconds), error.slice(0, 500)],
    );
  }

  return {
    listDestinations,
    getDestination,
    listOwnerRules,
    listLastForwardedAt,
    activeTargets,
    replaceOwnerRules,
    enqueueDelivery,
    reclaimExpiredClaims,
    claimDue,
    markSucceeded,
    markFailed,
  };
}

export type PacketShareRepository = ReturnType<typeof createPacketShareRepository>;
