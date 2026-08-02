import type { QueryResultRow } from 'pg';

type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export async function ownerAlertRuleRows(
  query: QueryFn,
  ownerUsername: string,
  nodeIds: string[],
) {
  return query<{
    id: string;
    node_id: string;
    rule_type: string;
    threshold: number;
    channels: { webhook?: string };
    enabled: boolean;
    pause_reason: string | null;
    last_triggered_at: string | null;
    last_delivery_success_at: string | null;
    last_delivery_error_at: string | null;
    last_delivery_error: string | null;
  }>(
    `SELECT id::text, node_id, rule_type, threshold, channels, enabled,
            pause_reason, last_triggered_at::text,
            last_delivery_success_at::text, last_delivery_error_at::text,
            last_delivery_error
     FROM owner_alert_rules
     WHERE owner_username = $1 AND node_id = ANY($2::text[])
     ORDER BY created_at`,
    [ownerUsername, nodeIds],
  );
}

export async function upsertOwnerAlertRule(
  query: QueryFn,
  input: {
    ownerUsername: string;
    nodeId: string;
    ruleType: string;
    threshold: number;
    webhook: string;
    enabled: boolean;
  },
) {
  return query(
    `INSERT INTO owner_alert_rules (
       owner_username, node_id, rule_type, threshold, channels, enabled
     )
     VALUES ($1, $2, $3, $4, $5::jsonb, $6)
     ON CONFLICT (owner_username, node_id, rule_type) DO UPDATE SET
       threshold = EXCLUDED.threshold, channels = EXCLUDED.channels,
       enabled = EXCLUDED.enabled, pause_reason = NULL,
       last_delivery_error = NULL, last_delivery_error_at = NULL,
       updated_at = NOW()
     RETURNING id::text, node_id, rule_type, threshold, enabled`,
    [
      input.ownerUsername,
      input.nodeId,
      input.ruleType,
      input.threshold,
      JSON.stringify(input.webhook ? { webhook: input.webhook } : {}),
      input.enabled,
    ],
  );
}

export async function ownerAlertDeliveryRows(
  query: QueryFn,
  ownerUsername: string,
  nodeIds: string[],
) {
  return query(
    `SELECT delivery.id::text, delivery.rule_id::text, rules.node_id,
            rules.rule_type, delivery.event_key, delivery.channel,
            delivery.destination_host, delivery.status, delivery.attempts,
            delivery.is_test, delivery.next_attempt_at::text,
            delivery.last_attempt_at::text, delivery.delivered_at::text,
            delivery.last_error, delivery.created_at::text,
            COALESCE(
              jsonb_agg(
                jsonb_build_object(
                  'attempt', attempt.attempt_number,
                  'outcome', attempt.outcome,
                  'httpStatus', attempt.http_status,
                  'error', attempt.error,
                  'startedAt', attempt.started_at,
                  'completedAt', attempt.completed_at
                )
                ORDER BY attempt.attempt_number DESC
              ) FILTER (WHERE attempt.id IS NOT NULL),
              '[]'::jsonb
            ) AS attempt_history
       FROM owner_alert_deliveries delivery
       JOIN owner_alert_rules rules ON rules.id = delivery.rule_id
       LEFT JOIN owner_alert_delivery_attempts attempt
         ON attempt.delivery_id = delivery.id
      WHERE rules.owner_username = $1
        AND rules.node_id = ANY($2::text[])
      GROUP BY delivery.id, rules.node_id, rules.rule_type
      ORDER BY delivery.created_at DESC
      LIMIT 100`,
    [ownerUsername, nodeIds],
  );
}

export async function deleteOwnerAlertRule(
  query: QueryFn,
  ruleId: string,
  ownerUsername: string,
  nodeIds: string[],
): Promise<void> {
  await query(
    `DELETE FROM owner_alert_rules
      WHERE id = $1 AND owner_username = $2 AND node_id = ANY($3::text[])`,
    [ruleId, ownerUsername, nodeIds],
  );
}
