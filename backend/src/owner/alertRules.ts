import { query } from '../db/index.js';

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
       AND (rules.last_triggered_at IS NULL OR rules.last_triggered_at < NOW() - INTERVAL '1 hour')`,
  );
  for (const rule of rules.rows) {
    const offlineMinutes = rule.last_seen ? (Date.now() - Date.parse(rule.last_seen)) / 60_000 : Number.POSITIVE_INFINITY;
    const triggered = rule.rule_type === 'offline_minutes' ? offlineMinutes >= rule.threshold
      : rule.rule_type === 'battery_below_mv' ? rule.battery_mv != null && rule.battery_mv <= rule.threshold
        : rule.path_loss_db != null && rule.path_loss_db >= rule.threshold;
    if (!triggered) continue;
    const webhook = String(rule.channels?.webhook ?? '').trim();
    if (webhook) {
      try {
        const response = await fetch(webhook, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            event: 'meshcore.owner.alert',
            nodeId: rule.node_id,
            ruleType: rule.rule_type,
            threshold: rule.threshold,
          }),
          signal: AbortSignal.timeout(8_000),
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
      } catch (error) {
        console.error('[owner-alerts] webhook failed', (error as Error).message);
        continue;
      }
    }
    await query('UPDATE owner_alert_rules SET last_triggered_at = NOW() WHERE id = $1', [rule.id]);
  }
}
