"""Coverage jobs: explicit worker subsystem ownership."""

from __future__ import annotations
import json
import math
import threading
import time
import datetime as dt
from typing import Callable, Optional
from rf.config import ANTENNA_HEIGHT_M
from rf.terrain import PermanentOutOfScope
import viewshed_queue_v2
from queue_lease import LeaseGuard
from worker_metrics import record_job
from coverage_model import calculate_viewshed
from coverage_repository import (
    already_calculated,
    store_coverage,
    store_permanent_coverage,
)
from link_jobs import enqueue_physical_link_jobs_for_node
from link_model import compute_planned_links
from link_repository import store_planned_links
from worker_connections import redis_connection, wait_for_db
from worker_math import check_job_budget, is_viewshed_eligible_coordinate, peak_rss_mb
from worker_types import Cancelled, InvalidJob
import logging

log = logging.getLogger("worker")

SIDE_EFFECT_MARKER_KEY = "viewshed:side-effects:{node_id}"

SIDE_EFFECT_MARKER_TTL_S = 90 * 24 * 60 * 60


def side_effects_complete(r_client, node_id: str, *, context) -> bool:
    try:
        return bool(r_client.exists(SIDE_EFFECT_MARKER_KEY.format(node_id=node_id)))
    except Exception:
        # If Redis is down we cannot confirm completion; treat as incomplete so
        # the caller replays (which will also fail and re-raise, NACKing the job).
        return False


def mark_side_effects_complete(r_client, node_id: str, *, context) -> None:
    r_client.set(
        SIDE_EFFECT_MARKER_KEY.format(node_id=node_id), "1", ex=SIDE_EFFECT_MARKER_TTL_S
    )


def replay_coverage_side_effects(db, r_client, node_id: str, *, context) -> None:
    """Re-enqueue link jobs and re-publish notifications for a node whose
    coverage row exists but whose post-commit side effects never completed."""
    with db.cursor() as cur:
        cur.execute(
            """SELECT geom, strength_geoms, radius_m, elevation_m
               FROM core.node_coverage nc
               LEFT JOIN core.nodes n ON n.node_id = nc.node_id
               WHERE nc.node_id = %s""",
            (node_id,),
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        log.warning(f"Cannot replay side effects for {node_id[:12]}…: no coverage row")
        return
    geom, strength_geoms, radius_m, elevation_m = row
    if context.settings.WORKER_MODE in ("all", "link"):
        _replay_link_jobs(db, r_client, node_id, radius_m, context=context)
    _publish_coverage_notifications(
        r_client, node_id, geom, strength_geoms, elevation_m, context=context
    )
    mark_side_effects_complete(r_client, node_id, context=context)
    log.info(f"Replayed missing side effects for {node_id[:12]}…")


def _replay_link_jobs(
    db, r_client, node_id: str, radius_m: Optional[float], *, context
) -> None:
    """Reload the node position and enqueue physical link jobs (idempotent)."""
    with db.cursor() as cur:
        cur.execute(
            "SELECT lat, lon FROM core.nodes WHERE node_id = %s",
            (node_id,),
        )
        row = cur.fetchone()
    if row is None or row[0] is None or row[1] is None:
        log.warning(f"Cannot replay link jobs for {node_id[:12]}…: no position")
        return
    enqueue_physical_link_jobs_for_node(
        db, r_client, node_id, row[0], row[1], radius_m, context=context
    )


def _publish_coverage_notifications(
    r_client, node_id: str, geom, strength_geoms, elevation_m, *, context
) -> None:
    r_client.publish(
        context.settings.LIVE_CHANNEL,
        json.dumps(
            {
                "type": "coverage_update",
                "data": {
                    "node_id": node_id,
                    "geom": geom,
                    "strength_geoms": strength_geoms,
                },
                "ts": int(time.time() * 1000),
            }
        ),
    )
    r_client.publish(
        context.settings.LIVE_CHANNEL,
        json.dumps(
            {
                "type": "node_upsert",
                "data": {
                    "node_id": node_id,
                    "elevation_m": round(float(elevation_m), 1)
                    if elevation_m is not None
                    else None,
                },
                "ts": int(time.time() * 1000),
            }
        ),
    )


def enqueue_uncovered(db, r_client, *, context):
    """On startup, queue all nodes that have a position but no coverage yet."""
    # Remove any coverage that was previously computed for hidden or non-repeater nodes.
    with db.cursor() as cur:
        cur.execute("""
            DELETE FROM core.node_coverage WHERE node_id IN (
                SELECT node_id FROM core.nodes
                WHERE name LIKE '%🚫%' OR (role IS NOT NULL AND role != 2)
            )
        """)
    db.commit()

    with db.cursor() as cur:
        cur.execute(
            """
            SELECT n.node_id, n.lat, n.lon
            FROM core.nodes n
            LEFT JOIN core.node_coverage nc ON n.node_id = nc.node_id
            WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
              AND n.lat BETWEEN %s AND %s
              AND n.lon BETWEEN %s AND %s
              AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
              AND (nc.node_id IS NULL OR nc.model_version < %s OR n.elevation_m IS NULL)
              AND (n.name IS NULL OR n.name NOT LIKE %s)
              AND (n.role IS NULL OR n.role = 2)
        """,
            (
                context.settings.UK_LAT_MIN,
                context.settings.UK_LAT_MAX,
                context.settings.UK_LON_MIN,
                context.settings.UK_LON_MAX,
                context.settings.COVERAGE_MODEL_VERSION,
                "%🚫%",
            ),
        )
        rows = cur.fetchall()
    if rows:
        log.info(
            f"Queuing {len(rows)} existing node(s) for viewshed calculation (model v{context.settings.COVERAGE_MODEL_VERSION})"
        )
        for node_id, lat, lon in rows:
            status, _ = viewshed_queue_v2.admit(
                r_client,
                {
                    "version": 2,
                    "node_id": node_id,
                    "lat": lat,
                    "lon": lon,
                    "model_version": context.settings.COVERAGE_MODEL_VERSION,
                },
            )
            if status not in ("accepted", "coalesced", "dead"):
                log.warning(
                    f"Coverage startup job {node_id[:12]}… was not admitted: {status}"
                )


def rebuild_pending_viewshed_set(r_client, *, context):
    """Rebuild the dedupe set from v2 state plus any legacy queue entries."""
    r_client.delete(context.settings.JOB_PENDING_SET)
    node_ids = [
        str(job_id)
        for job_id, state in r_client.hgetall(viewshed_queue_v2.STATES).items()
        if state in ("queued", "in_flight", "dead")
    ]
    raw_jobs = r_client.lrange(context.settings.JOB_QUEUE, 0, -1)
    for raw in raw_jobs:
        try:
            job = json.loads(raw)
        except Exception:
            continue
        node_id = str(job.get("node_id") or "").strip()
        if node_id:
            node_ids.append(node_id)
    if node_ids:
        r_client.sadd(context.settings.JOB_PENDING_SET, *node_ids)
    log.info(
        f"Rebuilt viewshed pending set from queue ({len(set(node_ids))} unique node(s))"
    )


def recover_planned_coverage_jobs(db, r_client, *, context) -> int:
    available = max(
        0,
        context.settings.PLANNED_COVERAGE_QUEUE_MAX
        - int(r_client.llen(context.settings.JOB_QUEUE)),
    )
    with db.cursor() as cur:
        cur.execute("DELETE FROM planned_coverage_handles WHERE expires_at <= NOW()")
        cur.execute(
            """SELECT job_id, lat, lon
                 FROM planned_coverage_jobs
                WHERE (
                        status = 'queued'
                        OR (status = 'running' AND heartbeat_at < NOW() - INTERVAL '60 seconds')
                      )
                  AND expires_at > NOW()
                ORDER BY created_at
                LIMIT %s""",
            (available,),
        )
        rows = cur.fetchall()
        cur.execute(
            """UPDATE planned_coverage_jobs
                  SET status = 'queued', heartbeat_at = NULL, updated_at = NOW()
                WHERE status = 'running'
                  AND heartbeat_at < NOW() - INTERVAL '60 seconds'
                  AND expires_at > NOW()"""
        )
        cur.execute(
            """DELETE FROM core.node_coverage coverage
                USING planned_coverage_jobs jobs
               WHERE coverage.node_id = jobs.job_id
                 AND coverage.is_planned = TRUE
                 AND jobs.expires_at <= NOW()"""
        )
        cur.execute(
            """DELETE FROM planned_coverage_jobs jobs
                WHERE jobs.expires_at <= NOW()
                  AND NOT EXISTS (
                    SELECT 1 FROM planned_coverage_handles handles
                     WHERE handles.job_id = jobs.job_id
                  )"""
        )
    db.commit()
    queued = 0
    for job_id, lat, lon in rows:
        status, _ = viewshed_queue_v2.admit(
            r_client,
            {
                "version": 2,
                "node_id": job_id,
                "planned_job_id": job_id,
                "lat": lat,
                "lon": lon,
                "model_version": context.settings.COVERAGE_MODEL_VERSION,
            },
        )
        if status in ("accepted", "coalesced"):
            queued += 1
    if queued:
        log.warning(f"Recovered {queued} durable planned coverage job(s)")
    return queued


class PlannedJobHeartbeat:
    def __init__(self, job_id: str, context):
        self.context = context
        self.job_id = job_id
        self.stop_event = threading.Event()
        self.thread = threading.Thread(
            target=self._run, name=f"planned-{job_id[:8]}", daemon=True
        )

    def _renew(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE planned_coverage_jobs
                      SET heartbeat_at = NOW(),
                          expires_at = GREATEST(expires_at, NOW() + (%s * INTERVAL '1 second')),
                          updated_at = NOW()
                    WHERE job_id = %s AND status = 'running' """,
                (self.context.settings.PLANNED_RESULT_TTL_S, self.job_id),
            )
            cur.execute(
                """UPDATE planned_coverage_handles
                      SET expires_at = GREATEST(expires_at, NOW() + (%s * INTERVAL '1 second'))
                    WHERE job_id = %s""",
                (self.context.settings.PLANNED_RESULT_TTL_S, self.job_id),
            )

    def _run(self):
        conn = None
        while not self.stop_event.wait(15):
            try:
                if conn is None or conn.closed:
                    conn = wait_for_db(context=self.context)
                self._renew(conn)
            except Exception as exc:
                log.warning(f"Planned job heartbeat reconnecting: {exc}")
                if conn is not None:
                    conn.close()
                conn = None
        if conn is not None:
            conn.close()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop_event.set()
        self.thread.join(timeout=5)


def process_planned_job(
    db,
    r_client,
    job: dict,
    *,
    cancellation_check: Optional[Callable[[], None]] = None,
    context,
) -> None:
    job_id = str(job["planned_job_id"])
    check_job_budget(None, cancellation_check, context=context)
    with db.cursor() as cur:
        cur.execute(
            """UPDATE planned_coverage_jobs
                  SET status = 'running', heartbeat_at = NOW(), error = NULL, updated_at = NOW()
                WHERE job_id = %s AND expires_at > NOW()
              RETURNING job_id""",
            (job_id,),
        )
        if cur.fetchone() is None:
            return
    db.commit()
    try:
        with PlannedJobHeartbeat(job_id, context):
            process_job(
                db,
                r_client,
                job,
                cancellation_check=cancellation_check,
                context=context,
            )
        check_job_budget(None, cancellation_check, context=context)
        expires_at = dt.datetime.now(dt.timezone.utc) + dt.timedelta(
            seconds=context.settings.PLANNED_RESULT_TTL_S
        )
        with db.cursor() as cur:
            cur.execute(
                """UPDATE planned_coverage_jobs
                      SET status = 'ready', heartbeat_at = NOW(), expires_at = %s,
                          error = NULL, updated_at = NOW()
                    WHERE job_id = %s""",
                (expires_at, job_id),
            )
            cur.execute(
                """UPDATE planned_coverage_handles SET expires_at = %s WHERE job_id = %s""",
                (expires_at, job_id),
            )
            cur.execute(
                """UPDATE core.node_coverage
                      SET is_planned = TRUE, expires_at = %s
                    WHERE node_id = %s""",
                (expires_at, job_id),
            )
        db.commit()
    except Cancelled:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    except PermanentOutOfScope as exc:
        with db.cursor() as cur:
            cur.execute(
                """UPDATE planned_coverage_jobs
                      SET status = 'permanent', error = %s, heartbeat_at = NOW(), updated_at = NOW()
                    WHERE job_id = %s""",
                (str(exc)[:500], job_id),
            )
        db.commit()
        log.info(f"Planned coverage job {job_id} has a permanent outcome: {exc}")
    except Exception as exc:
        with db.cursor() as cur:
            cur.execute(
                """UPDATE planned_coverage_jobs
                      SET status = 'failed', error = %s, heartbeat_at = NOW(), updated_at = NOW()
                    WHERE job_id = %s""",
                (str(exc)[:500], job_id),
            )
        db.commit()
        raise


RELEASE_VIEWSHED_PAYLOAD_SCRIPT = """
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current and current == ARGV[2] then
  redis.call('HDEL', KEYS[1], ARGV[1])
  local bytes = math.max(0, tonumber(redis.call('HGET', KEYS[2], 'bytes') or '0') - string.len(ARGV[2]))
  redis.call('HSET', KEYS[2], 'bytes', bytes)
  return bytes
end
return tonumber(redis.call('HGET', KEYS[2], 'bytes') or '0')
"""


def release_viewshed_queue_payload(
    r_client, job: dict, raw_payload: str, *, context
) -> int:
    """Release producer-side byte accounting immediately after BRPOP."""
    node_id = str(job.get("node_id") or "")
    if not node_id:
        return 0
    return int(
        r_client.eval(
            RELEASE_VIEWSHED_PAYLOAD_SCRIPT,
            2,
            context.settings.JOB_PAYLOADS,
            context.settings.JOB_COUNTERS,
            node_id,
            raw_payload,
        )
    )


def process_job(
    db,
    r_client,
    job: dict,
    *,
    cancellation_check: Optional[Callable[[], None]] = None,
    context,
):
    job_started = time.monotonic()
    deadline_monotonic = job_started + context.settings.COVERAGE_JOB_DEADLINE_SECONDS
    stage_seconds: dict[str, float] = {}
    check_job_budget(deadline_monotonic, cancellation_check, context=context)
    node_id = str(job.get("node_id") or "").strip()
    if not node_id or len(node_id) > 128:
        raise InvalidJob("node_id is required and must be at most 128 characters")
    is_planned = bool(node_id.startswith("plan_") or job.get("planned_job_id"))
    try:
        lat = float(job["lat"])
        lon = float(job["lon"])
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidJob("lat and lon must be finite numbers") from exc
    if not all(math.isfinite(value) for value in (lat, lon)):
        raise InvalidJob("lat and lon must be finite numbers")
    try:
        if not is_viewshed_eligible_coordinate(lat, lon, context=context):
            raise PermanentOutOfScope(
                f"coordinate outside UK coverage bounds at ({lat:.4f}, {lon:.4f})"
            )
        # Skip hidden (🚫) or non-repeater nodes regardless of how the job arrived
        with db.cursor() as cur:
            cur.execute(
                """SELECT n.name, n.role, nc.antenna_height_m
                   FROM core.nodes n
                   LEFT JOIN core.node_coverage nc ON nc.node_id = n.node_id
                   WHERE n.node_id = %s""",
                (node_id,),
            )
            row = cur.fetchone()
        node_antenna_height_m = ANTENNA_HEIGHT_M
        if row:
            name, role, stored_antenna_h = row
            if stored_antenna_h is not None:
                node_antenna_height_m = float(stored_antenna_h)
            if name and "🚫" in name:
                log.info(f"Skipping hidden node {node_id[:12]}…")
                return
            if role is not None and role != 2:
                log.info(f"Skipping non-repeater {node_id[:12]}… (role={role})")
                return

        if already_calculated(
            db, node_id, require_predicted_links=is_planned, context=context
        ):
            if not is_planned and not side_effects_complete(
                r_client, node_id, context=context
            ):
                # BUG-006: coverage exists but the post-commit side effects
                # (link jobs + notifications) never completed — replay them
                # instead of silently skipping.
                replay_coverage_side_effects(db, r_client, node_id, context=context)
            log.info(f"Coverage already exists for {node_id[:12]}…, skipping")
            return

        log.info(f"Viewshed: {node_id[:12]}… at ({lat:.4f}, {lon:.4f})")
        result = calculate_viewshed(
            node_id,
            lat,
            lon,
            antenna_height_m=node_antenna_height_m,
            deadline_monotonic=deadline_monotonic,
            cancellation_check=cancellation_check,
            context=context,
        )
        stage_seconds.update(result.stage_seconds)
        geom = result.geom
        strength_geoms = result.strength_geoms
        radius_m = result.radius_m
        elevation_m = result.elevation_m
        predicted: Optional[list[dict]] = None
        if is_planned:
            # Planned (hypothetical) repeater: predict viable links to nearby real
            # repeaters in-line so the result is ready when the user polls coverage.
            predicted = compute_planned_links(
                db,
                lat,
                lon,
                elevation_m,
                radius_m,
                antenna_height_m=node_antenna_height_m,
                stage_seconds=stage_seconds,
                deadline_monotonic=deadline_monotonic,
                cancellation_check=cancellation_check,
                context=context,
            )
        check_job_budget(deadline_monotonic, cancellation_check, context=context)
        db_write_started = time.monotonic()
        store_coverage(
            db,
            node_id,
            geom,
            strength_geoms,
            radius_m,
            elevation_m,
            antenna_height_m=node_antenna_height_m,
            commit=not is_planned,
            context=context,
        )
        if is_planned:
            assert predicted is not None
            store_planned_links(db, node_id, predicted)
            log.info(f"Planned repeater {node_id}: {len(predicted)} predicted link(s)")
        elif context.settings.WORKER_MODE in ("all", "link"):
            queued_links = enqueue_physical_link_jobs_for_node(
                db, r_client, node_id, lat, lon, radius_m, context=context
            )
            if queued_links > 0:
                log.info(
                    f"Queued {queued_links} physical link job(s) for {node_id[:12]}…"
                )
        stage_seconds["database_write"] = round(
            time.monotonic() - db_write_started,
            6,
        )
        check_job_budget(deadline_monotonic, cancellation_check, context=context)
        duration_seconds = round(time.monotonic() - job_started, 6)
        log.info(
            "coverage_job_metrics %s",
            json.dumps(
                {
                    "node_id": node_id,
                    "is_planned": is_planned,
                    "model": context.settings.COVERAGE_MODEL,
                    "model_version": context.settings.COVERAGE_MODEL_VERSION,
                    "duration_seconds": duration_seconds,
                    "peak_rss_mb": max(result.peak_rss_mb, peak_rss_mb()),
                    "stage_seconds": stage_seconds,
                    "predicted_link_count": len(predicted or []),
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        log.info(f"Done in {duration_seconds:.1f}s — notifying frontend")

        _publish_coverage_notifications(
            r_client, node_id, geom, strength_geoms, elevation_m, context=context
        )
        if not is_planned:
            # BUG-006: only after EVERY side effect succeeds do we record the
            # completion marker, so a crash/Redis failure here is detected on
            # retry and the side effects replayed instead of skipped forever.
            mark_side_effects_complete(r_client, node_id, context=context)
    except PermanentOutOfScope as exc:
        store_permanent_coverage(
            db, node_id, str(exc), is_planned=is_planned, context=context
        )
        raise


def process_claimed_viewshed_job(db, r_client, claimed, *, context) -> None:
    job_id, token, job, attempt = claimed
    permanent_outcome: Optional[PermanentOutOfScope] = None
    try:
        with LeaseGuard(
            lambda: redis_connection(context=context),
            job_id,
            token,
            viewshed_queue_v2.LEASE_PROTOCOL,
        ) as renewer:
            try:
                if job.get("planned_job_id"):
                    process_planned_job(
                        db,
                        r_client,
                        job,
                        cancellation_check=renewer.assert_owned,
                        context=context,
                    )
                else:
                    process_job(
                        db,
                        r_client,
                        job,
                        cancellation_check=renewer.assert_owned,
                        context=context,
                    )
            except PermanentOutOfScope as exc:
                # process_job persists this explicit terminal result before
                # raising. It is therefore safe to ACK after the lease fence.
                permanent_outcome = exc
            renewer.assert_owned()
            if not viewshed_queue_v2.owns_lease(r_client, job_id, token):
                raise RuntimeError(f"VIEWSHED_LEASE_LOST:{job_id}")
        transition = viewshed_queue_v2.ack(r_client, job_id, token)
        if transition == "invalid":
            raise RuntimeError(f"VIEWSHED_ACK_REJECTED:{job_id}")
        if permanent_outcome is not None:
            log.info(
                f"Coverage job {job_id[:12]}… completed permanently: {permanent_outcome}"
            )
        elif transition == "requeued":
            log.info(
                f"Coverage job {job_id[:12]}… coalesced a newer position and was requeued"
            )
        record_job("viewshed", "job", "success")
    except InvalidJob:
        try:
            db.rollback()
        except Exception:
            pass
        transition = viewshed_queue_v2.nack(
            r_client,
            job_id,
            token,
            permanent=True,
            reason="InvalidJob",
        )
        record_job("viewshed", "job", "dead")
        log.exception(
            f"Invalid coverage job {job_id[:12]}… moved to terminal state; "
            f"attempt={attempt} transition={transition}"
        )
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        transition = viewshed_queue_v2.nack(
            r_client,
            job_id,
            token,
            reason=type(exc).__name__,
        )
        record_job("viewshed", "job", "failure")
        log.exception(
            f"Coverage job {job_id[:12]}… failed; attempt={attempt} transition={transition}"
        )
        raise
