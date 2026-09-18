#!/usr/bin/env python3
"""UKMesh realtime message tagger - TypeSafe/Jev.

Polls the packets table for new public-channel messages, asks Jev to tag each
one (speaker / kind / topic + flags), and stores the result in message_tags.
Tags are displayed on the public feed and app.ukmesh.com detail panels; private
or out-of-scope packets are never sent to the API (public visibility predicate +
network scope applied in the query). Monitoring data lives in tagger_state
(heartbeat + counters).

Failure handling: a message that fails all inline API attempts is persisted to
tagger_retry and retried on every subsequent poll (up to TAGGER_MAX_RETRY_ATTEMPTS)
so a TypeSafe outage cannot silently drop tags (2026-09-17 hardening; the old
20s overlap window lost 6 messages during two 503 bursts).
"""
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request

import psycopg2

API_URL = os.environ.get('TYPESAFE_API_URL', 'https://api.typesafe.ai/v1/systemone')
API_KEY = os.environ.get('TYPESAFE_API_KEY', '').strip()
DATABASE_URL = os.environ['DATABASE_URL']
POLL_S = float(os.environ.get('TAGGER_POLL_S', '5'))
CALL_FLOOR_S = float(os.environ.get('TAGGER_CALL_FLOOR_S', '0.5'))
BATCH_LIMIT = int(os.environ.get('TAGGER_BATCH_LIMIT', '200'))
OVERLAP_S = int(os.environ.get('TAGGER_OVERLAP_S', '120'))
MAX_RETRY_ATTEMPTS = int(os.environ.get('TAGGER_MAX_RETRY_ATTEMPTS', '30'))
RETRY_BATCH = int(os.environ.get('TAGGER_RETRY_BATCH', '20'))
MODEL = os.environ.get('TYPESAFE_MODEL', 'jev-latest')
START = os.environ.get('TAGGER_START', '').strip()
NETWORKS = [n.strip() for n in os.environ.get('TAGGER_NETWORKS', 'ukmesh,northeast,teesside').split(',') if n.strip()]

QUESTIONS = {
    "speaker": {"type": "choice", "instructions": "Who most likely sent this message?",
                "criteria": {"human": "A human operator typing a message",
                             "bot": "An automated bot or script",
                             "room-server": "A room server, repeater, or bridge auto-response",
                             "unclear": "Cannot tell from the message"}},
    "kind": {"type": "choice", "instructions": "What is the primary kind of this message?",
             "criteria": {"chat": "Social conversation, greeting, banter, or sharing (links, pictures, files, images)",
                          "radio-check": "Checking whether/how their signal is received",
                          "test-ping": "A test or ping",
                          "ack": "Acknowledgement of a previous signal",
                          "position": "Sharing their whereabouts or movements (e.g. 'I am near X', 'heading to Y', 'going off air for a bit') - not announcements about infrastructure or network status",
                          "coverage": "Reporting range, hops, or propagation",
                          "help": "Asking for help or information from others",
                          "noise": "No meaningful content (gibberish, stray characters, emoji only)",
                          "spam": "Only unsolicited promotion, scams, or abuse. IMPORTANT: a bare link, picture, or file with no promotional text is NEVER spam (mesh users share links/images routinely); when choosing between spam and chat for a bare link, choose chat",
                          "automation": "Automated telemetry, home-automation, or sensor notification",
                          "command": "A command to a system or bot (e.g. !test, !path, !help)"}},
    "topic": {"type": "choice", "instructions": "What is this message mainly about?",
              "criteria": {"weather": "Weather or conditions",
                           "meetups": "Meetups, events, or social plans",
                           "travel": "Travel, journeys, or being on the move",
                           "gear": "Radios, antennas, equipment, or tech gear",
                           "network": "Network coverage, repeaters, hops, or connectivity",
                           "help": "Getting help, support, or answering questions",
                           "humour": "Jokes, banter, or humour",
                           "general": "General chatter that fits nothing else"}},
    "mentions_location": {"type": "noul", "instructions": "Does the message mention a geographic place (town, area, or landmark)?"},
    "directed": {"type": "noul", "instructions": "Is the message addressed to a specific person or node (by name or @mention)?"},
    "safety": {"type": "noul", "instructions": "Is this message safety or emergency related?"},
}

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
"""

_running = True


def _stop(*_a):
    global _running
    _running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def log(msg):
    print(f"[tagger] {msg}", flush=True)


def api_tag(state):
    payload = {'state': state, 'model': MODEL, 'questions': QUESTIONS}
    req = urllib.request.Request(API_URL, data=json.dumps(payload).encode(),
                                 headers={'Authorization': f'Bearer {API_KEY}',
                                          'Content-Type': 'application/json'})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=90) as r:
        resp = json.loads(r.read().decode())
    return resp, (time.time() - t0) * 1000.0


def flatten(answers):
    tags, conf = {}, {}
    for k, v in (answers or {}).items():
        if not isinstance(v, dict):
            continue
        if 'choice' in v:
            tags[k] = v.get('choice')
        elif 'noul' in v:
            tags[k] = v.get('noul')
        elif 'score' in v:
            tags[k] = v.get('score')
        if 'confidence' in v:
            conf[k] = v.get('confidence')
    return tags, conf


def tag_message(cur, ph, ts):
    """Tag one message with up to 3 inline attempts. Returns (ok, latency_ms, err)."""
    cur.execute("""
      SELECT payload->'decrypted'->>'sender', payload->'decrypted'->>'message'
      FROM packets WHERE packet_hash=%s LIMIT 1
    """, (ph,))
    row = cur.fetchone()
    if row is None or row[1] is None:
        return True, None, None  # nothing taggable (row vanished / no message)
    sender, message = row
    state = f"Message from node '{sender or 'unknown'}': \"{(message or '')[:4000]}\""
    for attempt in (1, 2, 3):
        try:
            resp, ms = api_tag(state)
            tags, conf = flatten(resp.get('answers'))
            cur.execute("""INSERT INTO message_tags
                           (packet_hash, packet_time, tagged_at, model, latency_ms, tags, confidence)
                           VALUES (%s, %s, now(), %s, %s, %s, %s)
                           ON CONFLICT (packet_hash) DO NOTHING""",
                        (ph, ts, MODEL, int(ms), json.dumps(tags), json.dumps(conf)))
            return True, ms, None
        except urllib.error.HTTPError as e:
            if e.code == 429:
                cur.execute("UPDATE tagger_state SET http_429=http_429+1 WHERE id=1")
                time.sleep(min(CALL_FLOOR_S * (2 ** attempt), 30))
                continue
            err = f'http {e.code}: {e.read().decode()[:160]}'
            time.sleep(2 * attempt)
        except Exception as ex:  # noqa: BLE001
            err = str(ex)[:200]
            time.sleep(2 * attempt)
    return False, None, err


def process_retries(cur):
    """Retry previously failed messages. Returns (ok_count, fail_count, pending)."""
    cur.execute("""
      SELECT packet_hash, packet_time, attempts FROM tagger_retry
      WHERE NOT exhausted AND attempts < %s
      ORDER BY first_failed_at ASC LIMIT %s
    """, (MAX_RETRY_ATTEMPTS, RETRY_BATCH))
    due = cur.fetchall()
    ok = fail = 0
    for ph, ts, attempts in due:
        cur.execute("SELECT 1 FROM message_tags WHERE packet_hash=%s", (ph,))
        if cur.fetchone():
            cur.execute("DELETE FROM tagger_retry WHERE packet_hash=%s", (ph,))
            continue
        good, ms, err = tag_message(cur, ph, ts)
        if good:
            cur.execute("DELETE FROM tagger_retry WHERE packet_hash=%s", (ph,))
            cur.execute("UPDATE tagger_state SET retried=retried+1 WHERE id=1")
            ok += 1
        else:
            fail += 1
            if attempts + 1 >= MAX_RETRY_ATTEMPTS:
                cur.execute("""UPDATE tagger_retry SET attempts=attempts+1, last_error=%s,
                               exhausted=true WHERE packet_hash=%s""", (err, ph))
                cur.execute("UPDATE tagger_state SET given_up=given_up+1 WHERE id=1")
            else:
                cur.execute("""UPDATE tagger_retry SET attempts=attempts+1, last_error=%s
                               WHERE packet_hash=%s""", (err, ph))
        time.sleep(CALL_FLOOR_S)
    cur.execute("SELECT count(*) FROM tagger_retry WHERE NOT exhausted")
    pending = cur.fetchone()[0]
    return ok, fail, pending


def main():
    if not API_KEY:
        log('FATAL: TYPESAFE_API_KEY is empty')
        sys.exit(2)
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(DDL)
    cur.execute("SELECT cursor_time FROM tagger_state WHERE id=1")
    cursor = cur.fetchone()[0]
    if cursor is None:
        if START:
            cur.execute("UPDATE tagger_state SET cursor_time=%s::timestamptz WHERE id=1", (START,))
        else:
            cur.execute("UPDATE tagger_state SET cursor_time=now() - interval '5 minutes' WHERE id=1")
        cur.execute("SELECT cursor_time FROM tagger_state WHERE id=1")
        cursor = cur.fetchone()[0]
    log(f"started; model={MODEL} poll={POLL_S}s floor={CALL_FLOOR_S}s overlap={OVERLAP_S}s "
        f"networks={','.join(NETWORKS)} cursor={cursor.isoformat()}")

    lat_hist = []
    last_log = 0.0
    while _running:
        loop_start = time.time()
        try:
            retry_ok, retry_fail, retry_pending = process_retries(cur)

            cur.execute("""
              SELECT * FROM (
                SELECT DISTINCT ON (packet_hash)
                       packet_hash, time,
                       payload->'decrypted'->>'sender' AS sender,
                       payload->'decrypted'->>'message' AS message
                FROM packets
                WHERE packet_type = 5
                  AND payload->'decrypted'->>'message' IS NOT NULL
                  AND time > %s::timestamptz - make_interval(secs => %s)
                  AND time <= now()
                  AND network = ANY(%s::text[])
                  AND visibility_ok IS TRUE
                  AND is_private IS NOT TRUE
                  AND EXISTS (
                    SELECT 1
                    FROM packet_visibility_materialization_state cached_visibility
                    JOIN public_visibility_state current_visibility
                      ON current_visibility.singleton = cached_visibility.singleton
                    WHERE cached_visibility.singleton = TRUE
                      AND cached_visibility.visibility_generation = current_visibility.generation
                  )
                ORDER BY packet_hash, time DESC
              ) x ORDER BY time ASC LIMIT %s
            """, (cursor, OVERLAP_S, NETWORKS, BATCH_LIMIT))
            rows = cur.fetchall()
            new = errs = 0
            max_t = cursor
            for ph, ts, sender, message in rows:
                if ts > max_t:
                    max_t = ts
                cur.execute("SELECT 1 FROM message_tags WHERE packet_hash=%s", (ph,))
                if cur.fetchone():
                    continue
                state = f"Message from node '{sender or 'unknown'}': \"{(message or '')[:4000]}\""
                ans, ms, err = None, None, None
                for attempt in (1, 2, 3):
                    try:
                        resp, ms = api_tag(state)
                        ans = resp.get('answers')
                        break
                    except urllib.error.HTTPError as e:
                        if e.code == 429:
                            cur.execute("UPDATE tagger_state SET http_429=http_429+1 WHERE id=1")
                            time.sleep(min(CALL_FLOOR_S * (2 ** attempt), 30))
                            continue
                        err = f'http {e.code}: {e.read().decode()[:160]}'
                        time.sleep(2 * attempt)
                    except Exception as ex:  # noqa: BLE001
                        err = str(ex)[:200]
                        time.sleep(2 * attempt)
                if ans:
                    tags, conf = flatten(ans)
                    cur.execute("""INSERT INTO message_tags
                                   (packet_hash, packet_time, tagged_at, model, latency_ms, tags, confidence)
                                   VALUES (%s, %s, now(), %s, %s, %s, %s)
                                   ON CONFLICT (packet_hash) DO NOTHING""",
                                (ph, ts, MODEL, int(ms), json.dumps(tags), json.dumps(conf)))
                    new += 1
                    lat_hist.append(ms)
                    lat_hist = lat_hist[-50:]
                else:
                    errs += 1
                    cur.execute("UPDATE tagger_state SET errors=errors+1, last_error=%s WHERE id=1", (err,))
                    # Persist for retry instead of letting the cursor move past it.
                    cur.execute("""INSERT INTO tagger_retry (packet_hash, packet_time, attempts, last_error)
                                   VALUES (%s, %s, 1, %s)
                                   ON CONFLICT (packet_hash) DO UPDATE
                                   SET attempts=tagger_retry.attempts+1, last_error=EXCLUDED.last_error""",
                                (ph, ts, err))
                time.sleep(CALL_FLOOR_S)
            if max_t and max_t != cursor:
                cur.execute("UPDATE tagger_state SET cursor_time=%s WHERE id=1", (max_t,))
                cursor = max_t
            avg = (sum(lat_hist) / len(lat_hist)) if lat_hist else None
            cur.execute("""UPDATE tagger_state SET seen=seen+%s, tagged=tagged+%s,
                           avg_latency_ms_50=%s, updated_at=now() WHERE id=1""",
                        (new + errs, new, avg))
            if new or errs or retry_ok or retry_fail or (time.time() - last_log) > 120:
                log(f"batch={len(rows)} new={new} errs={errs} retry_ok={retry_ok} "
                    f"retry_pending={retry_pending} avg50={avg and round(avg)}ms cursor={cursor.isoformat()}")
                last_log = time.time()
            if new + errs == 0 and retry_ok + retry_fail == 0:
                time.sleep(POLL_S)
        except Exception as ex:  # noqa: BLE001
            log(f"loop error: {ex}")
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(POLL_S)
            try:
                conn = psycopg2.connect(DATABASE_URL)
                conn.autocommit = True
                cur = conn.cursor()
            except Exception as ex2:  # noqa: BLE001
                log(f"reconnect failed: {ex2}")
                time.sleep(15)
        _ = loop_start
    log('shutting down')


if __name__ == '__main__':
    main()
