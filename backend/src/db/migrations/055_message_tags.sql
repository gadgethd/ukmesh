-- 055: message_tags + tagger bookkeeping schema.
--
-- The core backend reads message_tags for feed enrichment, packet detail and
-- the WebSocket initial state, but the table was previously created only by
-- tagger_worker.py's startup DDL -- which runs after a TYPESAFE_API_KEY gate
-- and exits before any DDL when the key is unset. Fresh stacks therefore had
-- no table: WS clients were closed with 1013 on connect and the CI smoke's
-- MQTT->WebSocket fanout check timed out.
--
-- The schema now lives in the migration ledger so every deployment has it
-- independently of the optional worker; the worker's CREATE TABLE IF NOT
-- EXISTS stays as a harmless no-op. Prefix 055 deliberately leaves 052-054
-- reserved for the owner packet sharing workstream.

SET LOCAL lock_timeout = '30s';
SET LOCAL statement_timeout = '0';

CREATE TABLE IF NOT EXISTS message_tags (
  packet_hash  text PRIMARY KEY,
  packet_time  timestamptz NOT NULL,
  tagged_at    timestamptz NOT NULL DEFAULT now(),
  model        text,
  latency_ms   integer,
  tags         jsonb NOT NULL,
  confidence   jsonb
);

CREATE TABLE IF NOT EXISTS tagger_state (
  id int PRIMARY KEY CHECK (id = 1),
  cursor_time timestamptz,
  seen bigint NOT NULL DEFAULT 0,
  tagged bigint NOT NULL DEFAULT 0,
  errors bigint NOT NULL DEFAULT 0,
  http_429 bigint NOT NULL DEFAULT 0,
  last_error text,
  avg_latency_ms_50 real,
  started_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz
);
INSERT INTO tagger_state (id) VALUES (1) ON CONFLICT DO NOTHING;
ALTER TABLE tagger_state ADD COLUMN IF NOT EXISTS retried bigint NOT NULL DEFAULT 0;
ALTER TABLE tagger_state ADD COLUMN IF NOT EXISTS given_up bigint NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS tagger_retry (
  packet_hash      text PRIMARY KEY,
  packet_time      timestamptz NOT NULL,
  attempts         int NOT NULL DEFAULT 0,
  last_error       text,
  first_failed_at  timestamptz NOT NULL DEFAULT now(),
  exhausted        boolean NOT NULL DEFAULT false
);
