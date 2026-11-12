"""Worker runtime: explicit worker subsystem ownership."""

from __future__ import annotations
from worker_context import WorkerContext
import argparse
import json
import multiprocessing
import os
import random
import threading
import time
import psycopg2
import redis
from osgeo import gdal
from rf.config import CALIBRATION_REFRESH_S
import link_queue_v3
import viewshed_queue_v2
from worker_metrics import heartbeat, start_worker_metrics
from coverage_jobs import (
    enqueue_uncovered,
    process_claimed_viewshed_job,
    process_job,
    process_planned_job,
    rebuild_pending_viewshed_set,
    recover_planned_coverage_jobs,
    release_viewshed_queue_payload,
)
from link_jobs import process_claimed_link_job, publish_link_updates
from link_model import process_link_job
from rf_context import (
    refresh_radio_neighbor_reports,
    refresh_rf_calibration,
    refresh_support_context,
)
from worker_connections import redis_connection, wait_for_db
from worker_diagnostics import run_test_mode
import logging

log = logging.getLogger("worker")


def link_idle_wait_seconds(now: float, last_link_reap: float, *, context=None) -> float:
    """Block until the next maintenance deadline, capped by lease reaping."""
    if context is None:
        context = WorkerContext()
    deadlines = [
        5.0 - (now - last_link_reap),
        context.settings.LINK_TOPOLOGY_REFRESH_S
        - (now - float(context.topology.snapshot.updated_at)),
        context.settings.SUPPORT_REFRESH_S
        - (now - float(context.rf.support.updated_at)),
        CALIBRATION_REFRESH_S - (now - float(context.rf.calibration.updated_at)),
    ]
    if context.settings.RADIO_BOT_URL:
        deadlines.append(
            context.settings.RADIO_NEIGHBOR_REFRESH_S
            - (now - float(context.rf.radio_updated_at))
        )
    return max(0.05, min(5.0, *deadlines))


def worker_loop(settings=None, *, context=None):
    """Single worker process: owns its own DB and Redis connections."""
    if context is None:
        context = WorkerContext()
    if settings is not None:
        context = WorkerContext(settings=settings)
    gdal.UseExceptions()
    name = multiprocessing.current_process().name
    db = wait_for_db(context=context)
    r_client = redis_connection(context=context)
    heartbeat_stop = threading.Event()
    if context.settings.WORKER_MODE in ("all", "link"):
        link_queue_v3.start_worker_heartbeat(
            lambda: redis_connection(context=context), heartbeat_stop
        )
    if context.settings.WORKER_MODE in ("all", "viewshed"):

        def viewshed_heartbeat():
            heartbeat_client = None
            while not heartbeat_stop.is_set():
                try:
                    if heartbeat_client is None:
                        heartbeat_client = redis_connection(context=context)
                    heartbeat_client.set(
                        context.settings.VIEWSHED_WORKER_HEARTBEAT,
                        str(int(time.time())),
                        ex=45,
                    )
                    heartbeat("viewshed")
                    heartbeat_stop.wait(10)
                except Exception as exc:
                    log.warning(f"Viewshed heartbeat reconnecting: {exc}")
                    if heartbeat_client is not None:
                        heartbeat_client.close()
                    heartbeat_client = None
                    heartbeat_stop.wait(2)
            if heartbeat_client is not None:
                heartbeat_client.close()

        threading.Thread(
            target=viewshed_heartbeat, name="viewshed-worker-heartbeat", daemon=True
        ).start()
    sync_radio_neighbors = name in ("MainProcess", "Worker-1")
    if sync_radio_neighbors and context.settings.WORKER_MODE in ("all", "link"):
        refresh_radio_neighbor_reports(db, r_client, force=True, context=context)
    refresh_rf_calibration(db, force=True, context=context)
    refresh_support_context(db, force=True, context=context)
    if context.settings.WORKER_MODE in ("all", "link"):
        context.topology.refresh(db, force=True)
    log.info(f"{name} ready")
    last_link_reap = 0.0
    last_viewshed_reap = 0.0
    last_planned_recovery = 0.0

    while True:
        try:
            if context.settings.WORKER_MODE in ("link", "viewshed"):
                heartbeat(context.settings.WORKER_MODE)
            if (
                context.settings.WORKER_MODE == "viewshed"
                and not context.settings.VIEWSHED_ENABLED
            ):
                time.sleep(300)
                continue
            if (
                context.settings.WORKER_MODE in ("all", "viewshed")
                and context.settings.VIEWSHED_ENABLED
            ):
                now = time.time()
                if now - last_planned_recovery >= 30:
                    recover_planned_coverage_jobs(db, r_client, context=context)
                    last_planned_recovery = now
                if now - last_viewshed_reap >= 5:
                    recovered = viewshed_queue_v2.reap(r_client)
                    if recovered:
                        log.warning(f"Recovered {recovered} expired coverage lease(s)")
                    viewshed_queue_v2.cleanup_dead(r_client)
                    last_viewshed_reap = now

            if sync_radio_neighbors and context.settings.WORKER_MODE in ("all", "link"):
                refresh_radio_neighbor_reports(db, r_client, context=context)
            refresh_rf_calibration(db, context=context)
            refresh_support_context(db, context=context)
            if context.settings.WORKER_MODE in ("all", "link"):
                context.topology.refresh(db)
                now = time.time()
                if now - last_link_reap >= 5:
                    recovered = link_queue_v3.reap(r_client)
                    if recovered:
                        log.warning(f"Recovered {recovered} expired link lease(s)")
                    link_queue_v3.cleanup_completed(r_client)
                    link_queue_v3.cleanup_dead(r_client)
                    last_link_reap = now
                # Drain v3 first. Claim removes a ready entry and installs its
                # token/lease in one Lua transition.
                while True:
                    claimed = link_queue_v3.claim(r_client)
                    if claimed is None:
                        break
                    process_claimed_link_job(db, r_client, claimed, context=context)
                # Compatible cutover: drain legacy jobs only after v3.
                if not r_client.exists(link_queue_v3.REBUILD):
                    while True:
                        raw = r_client.rpop(context.settings.LINK_JOB_QUEUE)
                        if raw is None:
                            break
                        updates = process_link_job(db, json.loads(raw), context=context)
                        publish_link_updates(r_client, updates, context=context)

            if (
                context.settings.WORKER_MODE in ("all", "viewshed")
                and context.settings.VIEWSHED_ENABLED
            ):
                claimed_viewshed = viewshed_queue_v2.claim(r_client)
                if claimed_viewshed is not None:
                    process_claimed_viewshed_job(
                        db, r_client, claimed_viewshed, context=context
                    )
                    continue

            if context.settings.WORKER_MODE == "viewshed":
                wait_queues = [context.settings.JOB_QUEUE]
            elif context.settings.WORKER_MODE == "link":
                r_client.blpop(
                    link_queue_v3.WAKE,
                    timeout=link_idle_wait_seconds(
                        time.time(), last_link_reap, context=context
                    ),
                )
                continue
            else:
                wait_queues = (
                    [context.settings.JOB_QUEUE, context.settings.LINK_JOB_QUEUE]
                    if context.settings.VIEWSHED_ENABLED
                    else [context.settings.LINK_JOB_QUEUE]
                )

            item = r_client.brpop(wait_queues, timeout=1)
            if item is None:
                continue
            queue_name, raw = item
            if queue_name == context.settings.LINK_JOB_QUEUE:
                updates = process_link_job(db, json.loads(raw), context=context)
                publish_link_updates(r_client, updates, context=context)
            else:
                job = json.loads(raw)
                release_viewshed_queue_payload(r_client, job, raw, context=context)
                try:
                    if job.get("planned_job_id"):
                        process_planned_job(db, r_client, job, context=context)
                    else:
                        process_job(db, r_client, job, context=context)
                finally:
                    r_client.srem(
                        context.settings.JOB_PENDING_SET, str(job.get("node_id") or "")
                    )
        except psycopg2.errors.DeadlockDetected as exc:
            log.warning(
                f"{name}: database deadlock detected — rolling back and recovering: {exc}"
            )
            try:
                db.rollback()
            except Exception:
                db = wait_for_db(context=context)
            time.sleep(random.uniform(0.25, 1.25))
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            log.warning(f"{name}: DB connection lost — reconnecting")
            try:
                if db is not None:
                    db.close()
            except Exception:
                pass
            db = wait_for_db(context=context)
            time.sleep(2)
        except Exception as exc:
            log.error(f"{name}: job error: {exc}")
            try:
                if db and not db.closed:
                    db.rollback()
            except Exception:
                try:
                    db = wait_for_db(context=context)
                except RuntimeError:
                    log.warning(f"{name}: DB still unavailable — retrying shortly")
            time.sleep(1)


def main(*, context=None):
    if context is None:
        context = WorkerContext()
    parser = argparse.ArgumentParser(description="MeshCore viewshed/link worker")
    parser.add_argument(
        "--test-pairs", help="Comma-separated node pairs in the form A:B,C:D"
    )
    parser.add_argument(
        "--test-from-node",
        help="Evaluate all current radio-bot neighbours for one repeater",
    )
    args = parser.parse_args()
    if args.test_pairs or args.test_from_node:
        run_test_mode(args, context=context)
        return

    start_worker_metrics(context.settings.WORKER_MODE)
    log.info(
        f"Viewshed worker starting (mode={context.settings.WORKER_MODE}, "
        f"coverage_model={context.settings.COVERAGE_MODEL}, model_version={context.settings.COVERAGE_MODEL_VERSION}, "
        f"viewshed_enabled={context.settings.VIEWSHED_ENABLED})"
    )
    context.settings.SRTM_DIR.mkdir(parents=True, exist_ok=True)

    # Connect once just to enqueue any nodes that lack coverage, then hand off
    # to the worker processes (each gets its own connection).
    db = wait_for_db(context=context)
    log.info("Connected to DB")
    r = redis.Redis.from_url(
        context.settings.REDIS_URL,
        decode_responses=True,
        password=context.settings.REDIS_PASSWORD,
    )
    r.ping()
    log.info("Connected to Redis")
    if (
        context.settings.WORKER_MODE in ("all", "viewshed")
        and context.settings.VIEWSHED_ENABLED
    ):
        rebuild_pending_viewshed_set(r, context=context)
        recover_planned_coverage_jobs(db, r, context=context)
        enqueue_uncovered(db, r, context=context)
    elif context.settings.WORKER_MODE == "viewshed":
        log.info("Viewshed disabled; skipping startup coverage backfill")
    db.close()

    num_workers = int(os.environ.get("NUM_WORKERS", "2"))
    log.info(f"Launching {num_workers} worker process(es)")

    if num_workers <= 1:
        worker_loop(context=context)
        return

    procs = [
        multiprocessing.Process(
            target=worker_loop,
            args=(context.settings,),
            name=f"Worker-{i + 1}",
            daemon=True,
        )
        for i in range(num_workers)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
