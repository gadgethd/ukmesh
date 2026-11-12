"""Link jobs: explicit worker subsystem ownership."""

from __future__ import annotations
from link_topology import TopologyCache
import hashlib
import json
import time
from dataclasses import asdict
from collections.abc import Iterable
from typing import Optional
import link_queue_v3
from queue_lease import LeaseGuard
from worker_metrics import record_job
from worker_types import LinkUpdate
from link_model import process_link_job
from worker_connections import redis_connection
from worker_math import node_dist_km, physical_candidate_radius_km
import logging

log = logging.getLogger("worker")


def publish_link_updates(r_client, updates: Iterable[LinkUpdate], *, context) -> None:
    for update in updates:
        r_client.publish(
            context.settings.LIVE_CHANNEL,
            json.dumps(
                {
                    "type": "link_update",
                    "data": asdict(update),
                    "ts": int(time.time() * 1000),
                }
            ),
        )


def process_claimed_link_job(db, r_client, claimed, *, context) -> None:
    job_id, token, job, _attempt = claimed
    updates = []
    try:
        with LeaseGuard(
            lambda: redis_connection(context=context),
            job_id,
            token,
            link_queue_v3.LEASE_PROTOCOL,
        ) as renewer:
            db.autocommit = False
            try:
                with db.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM ops.link_job_commits WHERE job_id = %s",
                        (job_id,),
                    )
                    already_committed = cur.fetchone() is not None
                    if not already_committed:
                        generation = str(job.get("generation") or "").strip()
                        cur.execute(
                            "SELECT pg_advisory_xact_lock(hashtext('meshcore-link-graph-write'))"
                        )
                        topology = (
                            TopologyCache(generation, settings=context.settings)
                            if generation
                            else TopologyCache(settings=context.settings)
                        )
                        if not generation:
                            topology.snapshot = context.topology.snapshot
                        updates = process_link_job(db, job, topology, context=context)
                        renewer.assert_owned()
                        if not link_queue_v3.owns_lease(r_client, job_id, token):
                            raise RuntimeError(f"LINK_LEASE_LOST:{job_id}")
                        payload_hash = hashlib.sha256(
                            json.dumps(
                                job, separators=(",", ":"), sort_keys=True
                            ).encode()
                        ).hexdigest()
                        cur.execute(
                            """INSERT INTO ops.link_job_commits
                                 (job_id, job_type, generation, logical_job_id, payload_hash)
                               VALUES (%s, %s, %s, %s, %s)
                               ON CONFLICT (job_id) DO NOTHING""",
                            (
                                job_id,
                                str(job.get("type") or "observe"),
                                generation or None,
                                job.get("logical_job_id"),
                                payload_hash,
                            ),
                        )
                db.commit()
                if not already_committed and not generation:
                    context.topology.snapshot = topology.snapshot
            except Exception:
                db.rollback()
                raise
            finally:
                db.autocommit = True
        # Pub/Sub is a post-commit live hint. Provisional graph writes and
        # rejected leases must never become visible through this channel.
        try:
            publish_link_updates(r_client, updates, context=context)
        except Exception:
            # The ledger already committed; retrying the SQL job cannot replay
            # this transient hint. Clients can reload the authoritative graph.
            log.warning(
                "Committed link job %s live notification failed", job_id, exc_info=True
            )
        if not link_queue_v3.ack(r_client, job_id, token):
            log.warning(f"Link v3 ACK rejected for {job_id}")
        record_job("link", "job", "success")
    except Exception as exc:
        result = link_queue_v3.nack(r_client, job_id, token, type(exc).__name__)
        record_job("link", "job", "failure")
        log.exception(f"Link v3 job {job_id} failed; transition={result}")
        raise


def enqueue_physical_link_jobs_for_node(
    db,
    r_client,
    node_id: str,
    lat: float,
    lon: float,
    radius_m: Optional[float],
    *,
    context,
) -> int:
    origin = {"lat": lat, "lon": lon, "radius_m": radius_m}
    origin_radius_km = physical_candidate_radius_km(radius_m, context=context)
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT n.node_id, n.lat, n.lon, nc.radius_m
            FROM core.nodes n
            LEFT JOIN core.node_coverage nc ON nc.node_id = n.node_id
            WHERE n.node_id <> %s
              AND n.lat IS NOT NULL
              AND n.lon IS NOT NULL
              AND n.lat BETWEEN %s AND %s
              AND n.lon BETWEEN %s AND %s
              AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
              AND (n.name IS NULL OR n.name NOT LIKE %s)
              AND (n.role IS NULL OR n.role = 2)
            """,
            (
                node_id,
                context.settings.UK_LAT_MIN,
                context.settings.UK_LAT_MAX,
                context.settings.UK_LON_MIN,
                context.settings.UK_LON_MAX,
                "%🚫%",
            ),
        )
        rows = cur.fetchall()

    queued = 0
    for peer_id, peer_lat, peer_lon, peer_radius_m in rows:
        peer = {"lat": peer_lat, "lon": peer_lon, "radius_m": peer_radius_m}
        if node_dist_km(origin, peer) > max(
            origin_radius_km,
            physical_candidate_radius_km(peer_radius_m, context=context),
        ):
            continue
        [a_id, b_id] = sorted((node_id, peer_id))
        status, _ = link_queue_v3.admit_physical(r_client, a_id, b_id)
        if status in ("accepted", "coalesced", "duplicate"):
            queued += 1
        else:
            log.warning(
                f"Physical link job not admitted for {a_id[:8]}…↔{b_id[:8]}…: {status}"
            )
    return queued
