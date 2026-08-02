import { randomUUID } from 'node:crypto';
import { query } from '../db/index.js';
import { deliverWebhook } from '../security/outboundWebhook.js';

const MAX_DELIVERY_ATTEMPTS = 5;
const CLAIM_LEASE_SECONDS = 120;

type Rule = {
  id: string;
  node_id: string;
  rule_type: 'offline_minutes' | 'battery_below_mv' | 'link_loss_above_db';
  threshold: number;
  channels: { webhook?: string };
  last_seen: string | null;
  battery_mv: number | null;
  path_loss_db: number | null;
};

type Delivery = {
  id: string;
  rule_id: string;
  destination_host: string;
  payload: Record<string, unknown>;
  webhook: string;
  attempts: number;
  claim_token: string;
  last_attempt_at: string;
};

export function ownerAlertTriggered(rule: Pick<
  Rule,
  'rule_type' | 'threshold' | 'last_seen' | 'battery_mv' | 'path_loss_db'
>, now = Date.now()): boolean {
  const offlineMinutes = rule.last_seen
    ? (now - Date.parse(rule.last_seen)) / 60_000
    : Number.POSITIVE_INFINITY;
  return rule.rule_type === 'offline_minutes' ? offlineMinutes >= rule.threshold
    : rule.rule_type === 'battery_below_mv' ? rule.battery_mv != null && rule.battery_mv <= rule.threshold
      : rule.path_loss_db != null && rule.path_loss_db >= rule.threshold;
}

export function deliveryFailureTransition(attempts: number): {
  status: 'failed' | 'dead_lettered';
  retryAfterSeconds: number;
} {
  if (attempts >= MAX_DELIVERY_ATTEMPTS) {
    return { status: 'dead_lettered', retryAfterSeconds: 0 };
  }
  return {
    status: 'failed',
    retryAfterSeconds: Math.min(3_600, 30 * (2 ** Math.max(0, attempts - 1))),
  };
}

function eventPayload(rule: Pick<Rule, 'node_id' | 'rule_type' | 'threshold'>, test: boolean) {
  return {
    event: test ? 'meshcore.owner.alert.test' : 'meshcore.owner.alert',
    nodeId: rule.node_id,
    ruleType: rule.rule_type,
    threshold: rule.threshold,
    test,
  };
}

async function queueDelivery(
  rule: Pick<Rule, 'id' | 'node_id' | 'rule_type' | 'threshold'>,
  webhook: string,
  eventKey: string,
  isTest: boolean,
): Promise<boolean> {
  const destinationHost = new URL(webhook).hostname.toLowerCase();
  const result = await query<{ id: string }>(
    `INSERT INTO owner_alert_deliveries
       (rule_id, event_key, channel, destination_host, payload, is_test)
     VALUES ($1, $2, 'webhook', $3, $4::jsonb, $5)
     ON CONFLICT (rule_id, event_key) DO NOTHING
     RETURNING id::text`,
    [
      rule.id,
      eventKey,
      destinationHost,
      JSON.stringify(eventPayload(rule, isTest)),
      isTest,
    ],
  );
  return result.rows.length === 1;
}

export async function queueOwnerTestDelivery(options: {
  ruleId: string;
  ownerUsername: string;
  ownedNodeIds: readonly string[];
  idempotencyKey?: string;
}): Promise<{ queued: boolean; eventKey: string }> {
  const ruleResult = await query<Pick<Rule, 'id' | 'node_id' | 'rule_type' | 'threshold' | 'channels'>>(
    `SELECT id::text, node_id, rule_type, threshold, channels
       FROM owner_alert_rules
      WHERE id = $1
        AND owner_username = $2
        AND node_id = ANY($3::text[])`,
    [options.ruleId, options.ownerUsername, options.ownedNodeIds],
  );
  const rule = ruleResult.rows[0];
  if (!rule) throw new Error('OWNER_ALERT_RULE_NOT_FOUND');
  const webhook = String(rule.channels?.webhook ?? '').trim();
  if (!webhook) throw new Error('OWNER_ALERT_WEBHOOK_NOT_CONFIGURED');
  const requestKey = options.idempotencyKey?.trim() || randomUUID();
  const eventKey = `test:${requestKey}`;
  return {
    queued: await queueDelivery(rule, webhook, eventKey, true),
    eventKey,
  };
}

async function claimDueDeliveries(): Promise<Delivery[]> {
  // A process crash cannot strand a delivery forever. A late claimant is
  // fenced by claim_token when it tries to commit its result.
  await query(
    `UPDATE owner_alert_deliveries
        SET status = 'failed',
            claim_token = NULL,
            claim_expires_at = NULL,
            next_attempt_at = NOW(),
            last_error = COALESCE(last_error, 'delivery claim expired'),
            updated_at = NOW()
      WHERE status = 'delivering'
        AND claim_expires_at <= NOW()`,
  );
  const claimed = await query<Delivery>(
    `WITH due AS (
       SELECT delivery.id, rules.channels->>'webhook' AS webhook
         FROM owner_alert_deliveries delivery
         JOIN owner_alert_rules rules ON rules.id = delivery.rule_id
        WHERE delivery.status IN ('pending', 'failed')
          AND delivery.attempts < $1
          AND delivery.next_attempt_at <= NOW()
          AND rules.enabled
          AND rules.pause_reason IS NULL
        ORDER BY delivery.next_attempt_at, delivery.id
        LIMIT 25
        FOR UPDATE SKIP LOCKED
     )
     UPDATE owner_alert_deliveries delivery
        SET status = 'delivering',
            attempts = attempts + 1,
            claim_token = md5(random()::text || clock_timestamp()::text || delivery.id::text),
            claim_expires_at = NOW() + ($2::text || ' seconds')::interval,
            last_attempt_at = NOW(),
            updated_at = NOW()
       FROM due
      WHERE delivery.id = due.id
      RETURNING delivery.id::text, delivery.rule_id::text,
                delivery.destination_host, delivery.payload, delivery.attempts,
                delivery.claim_token, delivery.last_attempt_at::text,
                due.webhook`,
    [MAX_DELIVERY_ATTEMPTS, String(CLAIM_LEASE_SECONDS)],
  );
  return claimed.rows.filter((row) => Boolean(String(row.webhook ?? '').trim()));
}

async function recordDeliverySuccess(
  delivery: Delivery,
  destinationHost: string,
  httpStatus: number,
): Promise<void> {
  await query(
    `WITH completed AS (
       UPDATE owner_alert_deliveries
          SET status = 'succeeded',
              delivered_at = NOW(),
              updated_at = NOW(),
              destination_host = $3,
              last_error = NULL,
              claim_token = NULL,
              claim_expires_at = NULL
        WHERE id = $1 AND claim_token = $2 AND status = 'delivering'
        RETURNING id, rule_id, attempts, channel, last_attempt_at
     ), attempt AS (
       INSERT INTO owner_alert_delivery_attempts
         (delivery_id, attempt_number, channel, outcome, destination_host,
          http_status, started_at)
       SELECT id, attempts, channel, 'succeeded', $3, $4, last_attempt_at
         FROM completed
       ON CONFLICT (delivery_id, attempt_number) DO NOTHING
       RETURNING delivery_id
     )
     UPDATE owner_alert_rules rules
        SET last_triggered_at = NOW(),
            last_delivery_success_at = NOW(),
            last_delivery_error = NULL,
            last_delivery_error_at = NULL,
            updated_at = NOW()
       FROM completed
      WHERE rules.id = completed.rule_id`,
    [delivery.id, delivery.claim_token, destinationHost, httpStatus],
  );
}

async function recordDeliveryFailure(delivery: Delivery, error: Error): Promise<void> {
  const transition = deliveryFailureTransition(delivery.attempts);
  const message = String(error.message).slice(0, 300);
  await query(
    `WITH completed AS (
       UPDATE owner_alert_deliveries
          SET status = $3,
              next_attempt_at = CASE
                WHEN $3 = 'failed'
                THEN NOW() + ($4::text || ' seconds')::interval
                ELSE next_attempt_at
              END,
              last_error = $5,
              claim_token = NULL,
              claim_expires_at = NULL,
              updated_at = NOW()
        WHERE id = $1 AND claim_token = $2 AND status = 'delivering'
        RETURNING id, rule_id, attempts, channel, destination_host, last_attempt_at
     ), attempt AS (
       INSERT INTO owner_alert_delivery_attempts
         (delivery_id, attempt_number, channel, outcome, destination_host,
          error, started_at)
       SELECT id, attempts, channel, 'failed', destination_host, $5, last_attempt_at
         FROM completed
       ON CONFLICT (delivery_id, attempt_number) DO NOTHING
       RETURNING delivery_id
     )
     UPDATE owner_alert_rules rules
        SET last_delivery_error_at = NOW(),
            last_delivery_error = $5,
            pause_reason = CASE
              WHEN $3 = 'dead_lettered' THEN 'delivery_dead_lettered'
              ELSE pause_reason
            END,
            updated_at = NOW()
       FROM completed
      WHERE rules.id = completed.rule_id`,
    [
      delivery.id,
      delivery.claim_token,
      transition.status,
      String(transition.retryAfterSeconds),
      message,
    ],
  );
  console.error('[owner-alerts] webhook failed', message);
}

async function deliverClaimed(delivery: Delivery): Promise<void> {
  try {
    const result = await deliverWebhook(delivery.webhook, delivery.payload);
    await recordDeliverySuccess(delivery, result.destinationHost, result.status);
  } catch (error) {
    await recordDeliveryFailure(delivery, error as Error);
  }
}

export async function deliverDueOwnerAlerts(): Promise<number> {
  const deliveries = await claimDueDeliveries();
  await Promise.all(deliveries.map(deliverClaimed));
  return deliveries.length;
}

export async function expireObserverRegistrationRequests(): Promise<number> {
  const result = await query<{ id: string }>(
    `UPDATE observer_registration_requests
        SET status = 'expired',
            decision_reason = COALESCE(decision_reason, 'Request expired after 90 days'),
            updated_at = NOW()
      WHERE status = 'pending'
        AND expires_at <= NOW()
      RETURNING id::text`,
  );
  return result.rows.length;
}

export async function pollOwnerAlertRules(): Promise<void> {
  const rules = await query<Rule>(
    `SELECT rules.id::text, rules.node_id, rules.rule_type, rules.threshold, rules.channels,
            nodes.last_seen::text,
            telemetry.battery_mv,
            links.path_loss_db
       FROM owner_alert_rules rules
       JOIN nodes ON nodes.node_id = rules.node_id
       LEFT JOIN LATERAL (
         SELECT battery_mv FROM node_status_samples
         WHERE node_id = rules.node_id ORDER BY time DESC LIMIT 1
       ) telemetry ON true
       LEFT JOIN LATERAL (
         SELECT MAX(itm_path_loss_db) AS path_loss_db FROM node_links
         WHERE node_a_id = rules.node_id OR node_b_id = rules.node_id
       ) links ON true
      WHERE rules.enabled
        AND rules.pause_reason IS NULL
        AND (rules.last_triggered_at IS NULL OR rules.last_triggered_at < NOW() - INTERVAL '1 hour')`,
  );
  for (const rule of rules.rows) {
    if (!ownerAlertTriggered(rule)) continue;
    const webhook = String(rule.channels?.webhook ?? '').trim();
    if (webhook) {
      const hourKey = new Date().toISOString().slice(0, 13);
      await queueDelivery(rule, webhook, `condition:${hourKey}`, false);
    } else {
      await query(
        `UPDATE owner_alert_rules
            SET last_triggered_at = NOW(),
                pause_reason = 'no_delivery_channel',
                updated_at = NOW()
          WHERE id = $1`,
        [rule.id],
      );
    }
  }
  await Promise.all([
    deliverDueOwnerAlerts(),
    expireObserverRegistrationRequests(),
  ]);
}
