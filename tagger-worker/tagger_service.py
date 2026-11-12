"""Tagging and retry orchestration. Reauthorize before every external attempt."""

import json

import time

import urllib.error

import tagger_repository as repository

from tagger_client import flatten


def tag_message(cur, ph, ts, rx_node_id, network, settings, classify, sleep=time.sleep):
    """Tag one message with up to 3 inline attempts.

    Returns (status, latency_ms, err) where status is 'tagged', 'skipped'
    (deleted, private, or no longer taggable) or 'failed'.
    """
    err = None
    for attempt in (1, 2, 3):
        row = repository.load_public_message(
            cur, ph, ts, rx_node_id, network, settings.networks
        )
        if row is None:
            return "skipped", None, None
        sender, message = row
        state = (
            f"Message from node '{sender or 'unknown'}': \"{(message or '')[:4000]}\""
        )
        try:
            resp, ms = classify(state)
            tags, conf = flatten(resp.get("answers"))
            cur.execute(
                """INSERT INTO message_tags
                           (packet_hash, packet_time, tagged_at, model, latency_ms, tags, confidence)
                           VALUES (%s, %s, now(), %s, %s, %s, %s)
                           ON CONFLICT (packet_hash) DO NOTHING""",
                (ph, ts, settings.model, int(ms), json.dumps(tags), json.dumps(conf)),
            )
            return "tagged", ms, None
        except urllib.error.HTTPError as e:
            if e.code == 429:
                cur.execute(
                    "UPDATE ops.tagger_state SET http_429=http_429+1 WHERE id=1"
                )
                sleep(min(settings.call_floor_s * (2**attempt), 30))
                continue
            err = f"http {e.code}: {e.read().decode()[:160]}"
            sleep(2 * attempt)
        except Exception as ex:  # noqa: BLE001
            err = str(ex)[:200]
            sleep(2 * attempt)
    return "failed", None, err


def process_retries(cur, settings, classify, sleep=time.sleep):
    """Retry previously failed messages. Returns (ok_count, fail_count, pending)."""
    cur.execute(
        """
      SELECT packet_hash, packet_time, attempts FROM ops.tagger_retry
      WHERE NOT exhausted AND attempts < %s
      ORDER BY first_failed_at ASC LIMIT %s
    """,
        (settings.max_retry_attempts, settings.retry_batch),
    )
    due = cur.fetchall()
    ok = fail = 0
    for ph, ts, attempts in due:
        cur.execute("SELECT 1 FROM message_tags WHERE packet_hash=%s", (ph,))
        if cur.fetchone():
            cur.execute("DELETE FROM ops.tagger_retry WHERE packet_hash=%s", (ph,))
            continue
        keys = repository.public_packet_keys(cur, ph, ts, settings.networks)
        if keys is None:
            # Became private (or vanished) after it was queued: never re-egress.
            cur.execute("DELETE FROM ops.tagger_retry WHERE packet_hash=%s", (ph,))
            continue
        status, ms, err = tag_message(cur, *keys, settings, classify, sleep)
        if status != "failed":
            cur.execute("DELETE FROM ops.tagger_retry WHERE packet_hash=%s", (ph,))
            if status == "tagged":
                cur.execute("UPDATE ops.tagger_state SET retried=retried+1 WHERE id=1")
                ok += 1
        else:
            fail += 1
            if attempts + 1 >= settings.max_retry_attempts:
                cur.execute(
                    """UPDATE ops.tagger_retry SET attempts=attempts+1, last_error=%s,
                               exhausted=true WHERE packet_hash=%s""",
                    (err, ph),
                )
                cur.execute(
                    "UPDATE ops.tagger_state SET given_up=given_up+1 WHERE id=1"
                )
            else:
                cur.execute(
                    """UPDATE ops.tagger_retry SET attempts=attempts+1, last_error=%s
                               WHERE packet_hash=%s""",
                    (err, ph),
                )
        sleep(settings.call_floor_s)
    cur.execute("SELECT count(*) FROM ops.tagger_retry WHERE NOT exhausted")
    pending = cur.fetchone()[0]
    return ok, fail, pending
