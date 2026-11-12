"""Public tagger reads and durable tagging state. No classifier or runtime setup."""

DDL = """
CREATE TABLE IF NOT EXISTS message_tags (
  packet_hash  text PRIMARY KEY,
  packet_time  timestamptz NOT NULL,
  tagged_at    timestamptz NOT NULL DEFAULT now(),
  model        text,
  latency_ms   integer,
  tags         jsonb NOT NULL,
  confidence   jsonb
);
CREATE TABLE IF NOT EXISTS ops.tagger_state (
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
INSERT INTO ops.tagger_state (id) VALUES (1) ON CONFLICT DO NOTHING;
ALTER TABLE ops.tagger_state ADD COLUMN IF NOT EXISTS retried bigint NOT NULL DEFAULT 0;
ALTER TABLE ops.tagger_state ADD COLUMN IF NOT EXISTS given_up bigint NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS ops.tagger_retry (
  packet_hash      text PRIMARY KEY,
  packet_time      timestamptz NOT NULL,
  attempts         int NOT NULL DEFAULT 0,
  last_error       text,
  first_failed_at  timestamptz NOT NULL DEFAULT now(),
  exhausted        boolean NOT NULL DEFAULT false
);
"""


PUBLIC_PACKET_PRIVACY_SQL = """
      AND visibility_ok IS TRUE
      AND is_private IS NOT TRUE
      AND EXISTS (
        SELECT 1
        FROM ops.packet_visibility_materialization_state cached_visibility
        JOIN ops.public_visibility_state current_visibility
          ON current_visibility.singleton = cached_visibility.singleton
        WHERE cached_visibility.singleton = TRUE
          AND cached_visibility.visibility_generation = current_visibility.generation
      )
"""


def load_public_message(cur, ph, ts, rx_node_id, network, networks):
    """Authorize immediately before each external attempt, including retries."""
    cur.execute(
        f"""
      WITH policy AS (
        SELECT EXISTS (
          SELECT 1 FROM ops.packet_visibility_materialization_state cached
          JOIN ops.public_visibility_state current ON current.singleton = cached.singleton
          WHERE cached.singleton = TRUE AND cached.visibility_generation = current.generation
        ) AND NOT EXISTS (
          SELECT 1 FROM ops.privacy_rematerialization_queue
          WHERE status IN ('pending', 'processing', 'failed')
        ) AS ready
      )
      SELECT policy.ready, candidate.sender, candidate.message FROM policy
      LEFT JOIN LATERAL (
        SELECT payload->'decrypted'->>'sender' AS sender,
               payload->'decrypted'->>'message' AS message
        FROM packets
        WHERE packet_hash=%s AND time=%s
          AND rx_node_id IS NOT DISTINCT FROM %s
          AND network IS NOT DISTINCT FROM %s AND network=ANY(%s::text[])
          AND packet_type=5 {PUBLIC_PACKET_PRIVACY_SQL}
          AND payload->'decrypted'->>'message' IS NOT NULL AND policy.ready
        ORDER BY time DESC LIMIT 1
      ) candidate ON TRUE
    """,
        (ph, ts, rx_node_id, network, list(networks)),
    )
    row = cur.fetchone()
    if row is None or row[0] is not True:
        raise RuntimeError("public visibility is unavailable")
    return row[1:] if row[2] is not None else None


def public_packet_keys(cur, ph, ts, networks):
    """Resolve the public packet identity for a queued retry.

    Returns None when the packet is no longer public (rematerialised private) or
    the row is gone. Unavailable policy must retain the retry for a later poll.
    """
    cur.execute(
        f"""
      WITH policy AS (
        SELECT EXISTS (
          SELECT 1 FROM ops.packet_visibility_materialization_state cached
          JOIN ops.public_visibility_state current ON current.singleton = cached.singleton
          WHERE cached.singleton = TRUE AND cached.visibility_generation = current.generation
        ) AND NOT EXISTS (
          SELECT 1 FROM ops.privacy_rematerialization_queue
          WHERE status IN ('pending', 'processing', 'failed')
        ) AS ready
      )
      SELECT policy.ready, candidate.* FROM policy
      LEFT JOIN LATERAL (
        SELECT packet_hash, time, rx_node_id, network FROM packets
        WHERE packet_hash=%s AND time=%s AND network=ANY(%s::text[])
          {PUBLIC_PACKET_PRIVACY_SQL} AND policy.ready
        ORDER BY rx_node_id NULLS LAST LIMIT 1
      ) candidate ON TRUE
    """,
        (ph, ts, list(networks)),
    )
    row = cur.fetchone()
    if row is None or row[0] is not True:
        raise RuntimeError("public visibility is unavailable")
    return row[1:] if row[1] is not None else None


CANDIDATE_SQL = f"""
              SELECT * FROM (
                SELECT DISTINCT ON (packet_hash)
                       packet_hash, time, rx_node_id, network,
                       payload->'decrypted'->>'sender' AS sender,
                       payload->'decrypted'->>'message' AS message
                FROM packets
                WHERE packet_type = 5
                  AND payload->'decrypted'->>'message' IS NOT NULL
                  AND time > %s::timestamptz - make_interval(secs => %s)
                  AND time <= now()
                  AND network = ANY(%s::text[])
                  {PUBLIC_PACKET_PRIVACY_SQL}
                  AND NOT EXISTS (SELECT 1 FROM message_tags tagged
                                  WHERE tagged.packet_hash = packets.packet_hash)
                  AND NOT EXISTS (SELECT 1 FROM ops.tagger_retry retry
                                  WHERE retry.packet_hash = packets.packet_hash)
                ORDER BY packet_hash, time DESC
              ) x ORDER BY time ASC LIMIT %s
            """


def candidates(cur, cursor, overlap_s, networks, batch_limit):
    cur.execute(CANDIDATE_SQL, (cursor, overlap_s, list(networks), batch_limit))

    return cur.fetchall()
