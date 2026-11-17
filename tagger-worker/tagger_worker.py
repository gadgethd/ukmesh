#!/usr/bin/env python3
"""UKMesh tagger process: settings, signals, polling and connection ownership."""

import argparse
from contextlib import closing
import signal
import sys
import time
import psycopg2
from tagger_config import TaggerSettings
import tagger_client
import tagger_repository as repository
import tagger_service

_running = True


def _stop(*_a):
    global _running
    _running = False


def log(msg):
    print(f"[tagger] {msg}", flush=True)


def connect_database(settings):
    conn = psycopg2.connect(
        settings.database_url, application_name=settings.database_application_name
    )
    try:
        conn.autocommit = True
        cur = conn.cursor()
        repository.validate_schema(cur)
        return conn, cur
    except Exception:
        conn.close()
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-schema", action="store_true",
        help="initialize a standalone tagger schema with an owner credential, then exit",
    )
    args = parser.parse_args(argv)
    settings = TaggerSettings.from_env()
    if args.bootstrap_schema:
        with closing(psycopg2.connect(
            settings.database_url,
            application_name=settings.database_application_name + "-bootstrap",
        )) as conn:
            with conn.cursor() as cur:
                repository.bootstrap_schema(cur)
            conn.commit()
        log("standalone schema initialized; start the worker with its runtime credential")
        return
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    classify = lambda state: tagger_client.classify(state, settings)
    if not settings.api_key:
        log("FATAL: TYPESAFE_API_KEY is empty")
        sys.exit(2)
    conn, cur = connect_database(settings)
    try:
        cur.execute("SELECT cursor_time FROM ops.tagger_state WHERE id=1")
        cursor = cur.fetchone()[0]
        if cursor is None:
            if settings.start:
                cur.execute(
                    "UPDATE ops.tagger_state SET cursor_time=%s::timestamptz WHERE id=1",
                    (settings.start,),
                )
            else:
                cur.execute(
                    "UPDATE ops.tagger_state SET cursor_time=now() - interval '5 minutes' WHERE id=1"
                )
            cur.execute("SELECT cursor_time FROM ops.tagger_state WHERE id=1")
            cursor = cur.fetchone()[0]
        log(
            f"started; model={settings.model} poll={settings.poll_s}s floor={settings.call_floor_s}s overlap={settings.overlap_s}s "
            f"networks={','.join(settings.networks)} cursor={cursor.isoformat()}"
        )

        lat_hist = []
        last_log = 0.0
        while _running:
            try:
                retry_ok, retry_fail, retry_pending = tagger_service.process_retries(
                    cur, settings, classify
                )

                rows = repository.candidates(
                    cur, cursor, settings.overlap_s, settings.networks, settings.batch_limit
                )
                new = errs = 0
                max_t = cursor
                for ph, ts, rx_node_id, network, sender, message in rows:
                    if ts > max_t:
                        max_t = ts
                    cur.execute("SELECT 1 FROM mesh.message_tags WHERE packet_hash=%s", (ph,))
                    if cur.fetchone():
                        continue
                    status, ms, err = tagger_service.tag_message(
                        cur, ph, ts, rx_node_id, network, settings, classify
                    )
                    if status == "skipped":
                        continue
                    if status == "tagged":
                        new += 1
                        lat_hist.append(ms)
                        lat_hist = lat_hist[-50:]
                    else:
                        errs += 1
                        cur.execute(
                            "UPDATE ops.tagger_state SET errors=errors+1, last_error=%s WHERE id=1",
                            (err,),
                        )
                        # Persist for retry instead of letting the cursor move past it.
                        cur.execute(
                            """INSERT INTO ops.tagger_retry (packet_hash, packet_time, attempts, last_error)
                                       VALUES (%s, %s, 1, %s)
                                       ON CONFLICT (packet_hash) DO UPDATE
                                       SET attempts=ops.tagger_retry.attempts+1, last_error=EXCLUDED.last_error""",
                            (ph, ts, err),
                        )
                    time.sleep(settings.call_floor_s)
                if max_t and max_t != cursor:
                    cur.execute(
                        "UPDATE ops.tagger_state SET cursor_time=%s WHERE id=1", (max_t,)
                    )
                    cursor = max_t
                avg = (sum(lat_hist) / len(lat_hist)) if lat_hist else None
                cur.execute(
                    """UPDATE ops.tagger_state SET seen=seen+%s, tagged=tagged+%s,
                               avg_latency_ms_50=%s, updated_at=now() WHERE id=1""",
                    (new + errs, new, avg),
                )
                if new or errs or retry_ok or retry_fail or (time.time() - last_log) > 120:
                    log(
                        f"batch={len(rows)} new={new} errs={errs} retry_ok={retry_ok} "
                        f"retry_pending={retry_pending} avg50={avg and round(avg)}ms cursor={cursor.isoformat()}"
                    )
                    last_log = time.time()
                if new + errs == 0 and retry_ok + retry_fail == 0:
                    time.sleep(settings.poll_s)
            except Exception as ex:  # noqa: BLE001
                log(f"loop error: {ex}")
                try:
                    conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(settings.poll_s)
                try:
                    conn.close()
                    conn, cur = connect_database(settings)
                except Exception as ex2:  # noqa: BLE001
                    log(f"reconnect failed: {ex2}")
                    time.sleep(15)
    finally:
        cur.close()
        conn.close()
    log("shutting down")


if __name__ == "__main__":
    main()
