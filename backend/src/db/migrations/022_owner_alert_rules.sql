CREATE TABLE IF NOT EXISTS owner_alert_rules (
  id BIGSERIAL PRIMARY KEY,
  owner_username TEXT NOT NULL,
  node_id TEXT NOT NULL,
  rule_type TEXT NOT NULL CHECK (rule_type IN ('offline_minutes', 'battery_below_mv', 'link_loss_above_db')),
  threshold DOUBLE PRECISION NOT NULL,
  channels JSONB NOT NULL DEFAULT '{}'::jsonb,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  last_triggered_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (owner_username, node_id, rule_type)
);
CREATE INDEX IF NOT EXISTS owner_alert_rules_enabled_idx ON owner_alert_rules(enabled, updated_at);
