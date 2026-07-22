CREATE TABLE IF NOT EXISTS operational_check_results (
  ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  check_name  TEXT        NOT NULL,
  status      TEXT        NOT NULL CHECK (status IN ('ok', 'failed')),
  latency_ms  INTEGER     NOT NULL CHECK (latency_ms >= 0),
  detail      TEXT
);

CREATE INDEX IF NOT EXISTS operational_check_results_check_ts_idx
  ON operational_check_results(check_name, ts DESC);
