-- Durable product operations: owner delivery audit, observer review,
-- single-job operator actions, and explicit planned-node publication.

ALTER TABLE owner_alert_rules
  ADD COLUMN IF NOT EXISTS pause_reason TEXT,
  ADD COLUMN IF NOT EXISTS last_delivery_success_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_delivery_error_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_delivery_error TEXT;

ALTER TABLE owner_alert_deliveries
  DROP CONSTRAINT IF EXISTS owner_alert_deliveries_status_check;
ALTER TABLE owner_alert_deliveries
  ADD CONSTRAINT owner_alert_deliveries_status_check
  CHECK (status IN ('pending', 'delivering', 'succeeded', 'failed', 'dead_lettered'));
ALTER TABLE owner_alert_deliveries
  ADD COLUMN IF NOT EXISTS channel TEXT NOT NULL DEFAULT 'webhook'
    CHECK (channel IN ('webhook')),
  ADD COLUMN IF NOT EXISTS claim_token TEXT,
  ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS is_test BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS owner_alert_delivery_attempts (
  id BIGSERIAL PRIMARY KEY,
  delivery_id BIGINT NOT NULL REFERENCES owner_alert_deliveries(id) ON DELETE CASCADE,
  attempt_number INTEGER NOT NULL CHECK (attempt_number BETWEEN 1 AND 5),
  channel TEXT NOT NULL CHECK (channel IN ('webhook')),
  outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'lease_lost')),
  destination_host TEXT NOT NULL,
  http_status INTEGER,
  error TEXT,
  started_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (delivery_id, attempt_number)
);
CREATE INDEX IF NOT EXISTS owner_alert_delivery_attempts_delivery_idx
  ON owner_alert_delivery_attempts (delivery_id, attempt_number DESC);

ALTER TABLE observer_registration_requests
  DROP CONSTRAINT IF EXISTS observer_registration_requests_status_check;
ALTER TABLE observer_registration_requests
  ADD CONSTRAINT observer_registration_requests_status_check
  CHECK (status IN ('pending', 'approved', 'rejected', 'expired', 'provisioned'));
ALTER TABLE observer_registration_requests
  ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '90 days'),
  ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS reviewed_by TEXT,
  ADD COLUMN IF NOT EXISTS decision_reason TEXT,
  ADD COLUMN IF NOT EXISTS duplicate_of BIGINT REFERENCES observer_registration_requests(id),
  ADD COLUMN IF NOT EXISTS provisioned_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS notification_status TEXT NOT NULL DEFAULT 'not_required'
    CHECK (notification_status IN ('not_required', 'pending', 'sent', 'failed')),
  ADD COLUMN IF NOT EXISTS notification_error TEXT;
CREATE INDEX IF NOT EXISTS observer_registration_queue_idx
  ON observer_registration_requests (status, created_at, id);
CREATE INDEX IF NOT EXISTS observer_registration_contact_idx
  ON observer_registration_requests (lower(contact), created_at DESC);

CREATE TABLE IF NOT EXISTS operator_audit_events (
  id BIGSERIAL PRIMARY KEY,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  request JSONB NOT NULL DEFAULT '{}'::jsonb,
  result JSONB,
  status TEXT NOT NULL DEFAULT 'started'
    CHECK (status IN ('started', 'succeeded', 'failed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS operator_audit_events_target_idx
  ON operator_audit_events (target_type, target_id, created_at DESC);

CREATE TABLE IF NOT EXISTS planned_node_publications (
  planned_node_id UUID PRIMARY KEY REFERENCES planned_nodes(id) ON DELETE CASCADE,
  public_name TEXT NOT NULL CHECK (char_length(public_name) BETWEEN 1 AND 100),
  public_lat DOUBLE PRECISION NOT NULL CHECK (public_lat BETWEEN -90 AND 90),
  public_lon DOUBLE PRECISION NOT NULL CHECK (public_lon BETWEEN -180 AND 180),
  public_height_m DOUBLE PRECISION CHECK (public_height_m BETWEEN 0 AND 500),
  region TEXT CHECK (region IS NULL OR region ~ '^[A-Z0-9]{2,8}$'),
  published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  published_by TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (expires_at > published_at)
);
CREATE INDEX IF NOT EXISTS planned_node_publications_public_idx
  ON planned_node_publications (expires_at, published_at DESC, planned_node_id);
