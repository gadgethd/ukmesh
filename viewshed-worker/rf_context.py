"""Rf context: explicit worker subsystem ownership."""

from __future__ import annotations
from dataclasses import replace
import math
import time
import datetime as dt
from typing import Optional
import numpy as np
import requests
from rf.config import (
    CALIBRATION_EXTRA_MARGIN_DB,
    CALIBRATION_MAX_THRESHOLD_BOOST_DB,
    CALIBRATION_MIN_LINKS,
    CALIBRATION_MIN_OBSERVED_COUNT,
    CALIBRATION_PERCENTILE,
    CALIBRATION_REFRESH_S,
    DEFAULT_USABLE_PATH_LOSS_DB,
    radio_snr_band,
)
import link_queue_v3
from worker_math import project_xy_km, weighted_quantile
import logging

log = logging.getLogger("worker")
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scipy.spatial import cKDTree


def refresh_rf_calibration(db, force: bool = False, *, context) -> None:
    RF_CALIBRATION = context.rf.calibration.values()
    now = time.time()
    if not force and now - float(RF_CALIBRATION["updated_at"]) < CALIBRATION_REFRESH_S:
        return

    with db.cursor() as cur:
        cur.execute(
            """SELECT
                 (SELECT MAX(p.time)
                    FROM mesh.packets p
                   WHERE p.snr IS NOT NULL
                     AND p.rx_node_id IS NOT NULL
                     AND p.src_node_id IS NOT NULL
                     AND p.rx_node_id <> p.src_node_id
                     AND p.hop_count = 0
                     AND p.time > NOW() - (%s * INTERVAL '1 hour')),
                 (SELECT MAX(nl.itm_computed_at) FROM core.node_links nl),
                 (SELECT COUNT(*) FROM core.node_links nl
                   WHERE nl.itm_path_loss_db IS NOT NULL AND nl.force_viable = false)""",
            (context.settings.RADIO_NEIGHBOR_MAX_AGE_HOURS,),
        )
        source_signature = cur.fetchone()
        if not force and source_signature == RF_CALIBRATION.get("source_signature"):
            RF_CALIBRATION["updated_at"] = now
            context.rf.install_calibration(RF_CALIBRATION)
            return
        cur.execute(
            """
            WITH link_packet_stats AS (
              SELECT
                LEAST(p.rx_node_id, p.src_node_id)    AS node_a_id,
                GREATEST(p.rx_node_id, p.src_node_id) AS node_b_id,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.snr) AS median_snr_db,
                COUNT(*)                               AS packet_count
              FROM mesh.packets p
              WHERE p.snr IS NOT NULL
                AND p.rx_node_id IS NOT NULL
                AND p.src_node_id IS NOT NULL
                AND p.rx_node_id <> p.src_node_id
                AND p.hop_count = 0
                AND p.time > NOW() - (%s * INTERVAL \'1 hour\')
              GROUP BY node_a_id, node_b_id
              HAVING COUNT(*) >= %s
            )
            SELECT
              nl.itm_path_loss_db,
              lps.packet_count    AS neighbor_report_count,
              lps.median_snr_db   AS neighbor_best_snr_db
            FROM core.node_links nl
            JOIN link_packet_stats lps
              ON lps.node_a_id = nl.node_a_id
             AND lps.node_b_id = nl.node_b_id
            WHERE nl.itm_path_loss_db IS NOT NULL
              AND nl.force_viable = false
            """,
            (
                context.settings.RADIO_NEIGHBOR_MAX_AGE_HOURS,
                CALIBRATION_MIN_OBSERVED_COUNT,
            ),
        )
        rows = cur.fetchall()

    RF_CALIBRATION["source_signature"] = source_signature

    if len(rows) < CALIBRATION_MIN_LINKS:
        RF_CALIBRATION["usable_path_loss_db"] = DEFAULT_USABLE_PATH_LOSS_DB
        RF_CALIBRATION["signal_thresholds_db"] = {
            "green": max(116.0, DEFAULT_USABLE_PATH_LOSS_DB - 16.0),
            "amber": max(124.0, DEFAULT_USABLE_PATH_LOSS_DB - 8.0),
            "red": DEFAULT_USABLE_PATH_LOSS_DB,
        }
        RF_CALIBRATION["samples"] = len(rows)
        RF_CALIBRATION["updated_at"] = now
        context.rf.install_calibration(RF_CALIBRATION)
        log.info(
            "RF calibration: insufficient observed links "
            f"({len(rows)}/{CALIBRATION_MIN_LINKS}) — using default threshold {DEFAULT_USABLE_PATH_LOSS_DB:.1f} dB"
        )
        return

    losses = np.asarray([float(row[0]) for row in rows], dtype=np.float64)
    counts = np.asarray([max(1.0, float(row[1])) for row in rows], dtype=np.float64)
    snrs = np.asarray(
        [float(row[2]) if row[2] is not None else -6.0 for row in rows],
        dtype=np.float64,
    )
    snr_weights = np.asarray(
        [
            1.75
            if radio_snr_band(snr) == "strong"
            else 1.35
            if radio_snr_band(snr) == "medium"
            else 1.10
            if radio_snr_band(snr) == "weak"
            else 0.75
            for snr in snrs
        ],
        dtype=np.float64,
    )
    weights = np.clip(np.sqrt(counts) * snr_weights, 0.5, 24.0)

    green_threshold = weighted_quantile(losses, weights, 0.35)
    amber_threshold = weighted_quantile(losses, weights, 0.65)
    observed_tail = weighted_quantile(losses, weights, CALIBRATION_PERCENTILE)
    usable_threshold = observed_tail + CALIBRATION_EXTRA_MARGIN_DB
    usable_threshold = min(
        DEFAULT_USABLE_PATH_LOSS_DB + CALIBRATION_MAX_THRESHOLD_BOOST_DB,
        usable_threshold,
    )
    usable_threshold = max(110.0, usable_threshold)
    green_threshold = min(green_threshold, usable_threshold)
    amber_threshold = min(max(green_threshold, amber_threshold), usable_threshold)

    RF_CALIBRATION["usable_path_loss_db"] = round(float(usable_threshold), 2)
    RF_CALIBRATION["signal_thresholds_db"] = {
        "green": round(float(green_threshold), 2),
        "amber": round(float(amber_threshold), 2),
        "red": round(float(usable_threshold), 2),
    }
    RF_CALIBRATION["samples"] = len(rows)
    RF_CALIBRATION["updated_at"] = now
    context.rf.install_calibration(RF_CALIBRATION)
    log.info(
        "RF calibration: "
        f"samples={len(rows)}, q35={green_threshold:.1f} dB, q65={amber_threshold:.1f} dB, "
        f"p{int(CALIBRATION_PERCENTILE * 100)}={observed_tail:.1f} dB, "
        f"usable={RF_CALIBRATION['usable_path_loss_db']:.1f} dB, "
        f"green={RF_CALIBRATION['signal_thresholds_db']['green']:.1f}, "
        f"amber={RF_CALIBRATION['signal_thresholds_db']['amber']:.1f}"
    )


def refresh_radio_neighbor_reports(
    db, r_client, force: bool = False, *, context
) -> None:
    if not context.settings.RADIO_BOT_URL:
        return

    now = time.time()
    if (
        not force
        and now - float(context.rf.radio_updated_at)
        < context.settings.RADIO_NEIGHBOR_REFRESH_S
    ):
        return

    try:
        response = requests.get(f"{context.settings.RADIO_BOT_URL}/state", timeout=10)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        log.warning(f"Radio neighbour sync failed: {exc}")
        context.rf.radio_updated_at = now
        return

    monitors = payload.get("monitors") if isinstance(payload, dict) else None
    if not isinstance(monitors, list):
        context.rf.radio_updated_at = now
        return

    imported = 0
    queued = 0
    with db.cursor() as cur:
        for monitor in monitors:
            if not isinstance(monitor, dict):
                continue
            reporter_id = (
                str(monitor.get("fullPublicKey") or monitor.get("targetHex") or "")
                .strip()
                .lower()
            )
            if len(reporter_id) != 64:
                continue

            neighbours_at_raw = monitor.get("lastNeighboursAt") or monitor.get(
                "lastSuccessAt"
            )
            base_seen_at: Optional[dt.datetime] = None
            if neighbours_at_raw:
                try:
                    base_seen_at = dt.datetime.fromisoformat(
                        str(neighbours_at_raw).replace("Z", "+00:00")
                    )
                except Exception:
                    base_seen_at = None

            last_neighbours = monitor.get("lastNeighbours")
            if not isinstance(last_neighbours, list):
                continue

            for neighbour in last_neighbours:
                if not isinstance(neighbour, dict):
                    continue
                peer_id = str(neighbour.get("fullPublicKey") or "").strip().lower()
                if len(peer_id) != 64 or peer_id == reporter_id:
                    continue
                snr_db = neighbour.get("snrDb")
                if not isinstance(snr_db, (int, float)):
                    continue

                seen_at = base_seen_at
                heard_seconds_ago = neighbour.get("heardSecondsAgo")
                if seen_at is not None and isinstance(heard_seconds_ago, (int, float)):
                    seen_at = seen_at - dt.timedelta(seconds=float(heard_seconds_ago))
                if seen_at is None:
                    seen_at = dt.datetime.now(dt.timezone.utc)

                a_id, b_id = sorted((reporter_id, peer_id))
                cur.execute(
                    """
                    INSERT INTO core.node_links (
                      node_a_id, node_b_id, observed_count, last_observed, count_a_to_b, count_b_to_a
                    )
                    VALUES (%s, %s, 0, NOW(), 0, 0)
                    ON CONFLICT (node_a_id, node_b_id) DO NOTHING
                    """,
                    (a_id, b_id),
                )
                cur.execute(
                    """
                    INSERT INTO core.node_link_radio_reports (
                      node_a_id, node_b_id, reporter_node_id, peer_node_id,
                      last_snr_db, best_snr_db, last_seen, sample_count
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
                    ON CONFLICT (node_a_id, node_b_id, reporter_node_id) DO UPDATE
                    SET peer_node_id = EXCLUDED.peer_node_id,
                        last_snr_db = CASE
                          WHEN EXCLUDED.last_seen >= node_link_radio_reports.last_seen
                            THEN EXCLUDED.last_snr_db
                          ELSE node_link_radio_reports.last_snr_db
                        END,
                        best_snr_db = GREATEST(
                          COALESCE(node_link_radio_reports.best_snr_db, EXCLUDED.best_snr_db),
                          EXCLUDED.best_snr_db
                        ),
                        last_seen = GREATEST(node_link_radio_reports.last_seen, EXCLUDED.last_seen),
                        sample_count = CASE
                          WHEN EXCLUDED.last_seen > node_link_radio_reports.last_seen
                            THEN node_link_radio_reports.sample_count + 1
                          ELSE node_link_radio_reports.sample_count
                        END
                    """,
                    (
                        a_id,
                        b_id,
                        reporter_id,
                        peer_id,
                        float(snr_db),
                        float(snr_db),
                        seen_at,
                    ),
                )
                status, _ = link_queue_v3.admit_physical(r_client, a_id, b_id)
                if status not in ("accepted", "coalesced", "duplicate"):
                    log.warning(f"Radio neighbour link job not admitted: {status}")
                imported += 1
                if status in ("accepted", "coalesced", "duplicate"):
                    queued += 1
    db.commit()
    context.rf.radio_updated_at = now
    if imported > 0:
        log.info(
            f"Radio neighbour sync: imported {imported} report(s), queued {queued} physical link job(s)"
        )


def refresh_support_context(db, force: bool = False, *, context) -> None:
    now = time.time()
    if (
        not force
        and now - float(context.rf.support.updated_at)
        < context.settings.SUPPORT_REFRESH_S
    ):
        return

    with db.cursor() as cur:
        cur.execute(
            """WITH positioned AS (
                 SELECT node_id, lat, lon
                 FROM core.nodes
                 WHERE lat IS NOT NULL
                   AND lon IS NOT NULL
                   AND lat BETWEEN %s AND %s
                   AND lon BETWEEN %s AND %s
                   AND NOT (ABS(lat) < 1e-9 AND ABS(lon) < 1e-9)
                   AND (name IS NULL OR name NOT LIKE %s)
                   AND (role IS NULL OR role = 2)
               ), viable AS (
                 SELECT node_a_id, node_b_id, itm_viable, force_viable
                 FROM core.node_links
                 WHERE itm_viable = true OR force_viable = true
               )
               SELECT
                 (SELECT md5(COALESCE(string_agg(
                    node_id || ':' || lat::text || ':' || lon::text,
                    ',' ORDER BY node_id), '')) FROM positioned),
                 (SELECT md5(COALESCE(string_agg(
                    node_a_id || ':' || node_b_id || ':' || itm_viable::text || ':' || force_viable::text,
                    ',' ORDER BY node_a_id, node_b_id), '')) FROM viable)""",
            (
                context.settings.UK_LAT_MIN,
                context.settings.UK_LAT_MAX,
                context.settings.UK_LON_MIN,
                context.settings.UK_LON_MAX,
                "%🚫%",
            ),
        )
        source_signature = cur.fetchone()
        if not force and source_signature == context.rf.support.source_signature:
            context.rf.support = replace(context.rf.support, updated_at=now)
            return
        cur.execute(
            """
            SELECT node_id, lat, lon
            FROM core.nodes
            WHERE lat IS NOT NULL
              AND lon IS NOT NULL
              AND lat BETWEEN %s AND %s
              AND lon BETWEEN %s AND %s
              AND NOT (ABS(lat) < 1e-9 AND ABS(lon) < 1e-9)
              AND (name IS NULL OR name NOT LIKE %s)
              AND (role IS NULL OR role = 2)
            """,
            (
                context.settings.UK_LAT_MIN,
                context.settings.UK_LAT_MAX,
                context.settings.UK_LON_MIN,
                context.settings.UK_LON_MAX,
                "%🚫%",
            ),
        )
        repeater_rows = cur.fetchall()
        cur.execute(
            """
            SELECT nl.node_a_id, nl.node_b_id,
                   na.lat, na.lon, nb.lat, nb.lon
            FROM core.node_links nl
            JOIN core.nodes na ON na.node_id = nl.node_a_id
            JOIN core.nodes nb ON nb.node_id = nl.node_b_id
            WHERE na.lat IS NOT NULL
              AND na.lon IS NOT NULL
              AND nb.lat IS NOT NULL
              AND nb.lon IS NOT NULL
              AND (nl.itm_viable = true OR nl.force_viable = true)
            """
        )
        link_rows = cur.fetchall()

    node_ids = [row[0] for row in repeater_rows]
    xy = (
        project_xy_km(
            [row[1] for row in repeater_rows],
            [row[2] for row in repeater_rows],
            context=context,
        )
        if repeater_rows
        else np.empty((0, 2))
    )
    max_link_km_by_node: dict[str, float] = {}
    for a_id, b_id, a_lat, a_lon, b_lat, b_lon in link_rows:
        cos_mid = math.cos(math.radians((a_lat + b_lat) / 2))
        dist_km = math.sqrt(
            ((a_lat - b_lat) * 111.32) ** 2 + ((a_lon - b_lon) * 111.32 * cos_mid) ** 2
        )
        if dist_km <= 0:
            continue
        max_link_km_by_node[a_id] = max(max_link_km_by_node.get(a_id, 0.0), dist_km)
        max_link_km_by_node[b_id] = max(max_link_km_by_node.get(b_id, 0.0), dist_km)
    context.rf.install_support(xy, node_ids, max_link_km_by_node, now, source_signature)
    if context.settings.WORKER_MODE != "link" and node_ids:
        get_support_tree(context=context)
    log.info(
        f"Mesh support context: repeaters={len(node_ids)}, "
        f"link-capped nodes={len(max_link_km_by_node)}"
    )


def source_support_radius_m(
    node_id: str, fallback_radius_m: float, *, context
) -> float:
    max_link_km = context.rf.support.max_link_km_by_node.get(node_id)
    if not max_link_km:
        return fallback_radius_m
    return min(
        fallback_radius_m,
        max(
            context.settings.RF_MIN_RADIUS_M,
            max_link_km * 1000.0 * context.settings.RF_SOURCE_LINK_RADIUS_MULTIPLIER,
        ),
    )


def get_support_tree(*, context) -> Optional[cKDTree]:
    tree = context.rf.support_tree
    coordinates = context.rf.support.coordinates
    if tree is None and coordinates is not None and len(coordinates) > 0:
        from scipy.spatial import cKDTree

        tree = cKDTree(coordinates)
        context.rf.support_tree = tree
    return tree


def support_penalty_db(
    source_node_id: str,
    sample_lats: np.ndarray,
    sample_lons: np.ndarray,
    *,
    context,
) -> np.ndarray:
    tree = get_support_tree(context=context)
    node_ids: list[str] = context.rf.support.node_ids
    source_index = context.rf.support.node_index_by_id.get(source_node_id)
    if tree is None or len(node_ids) < 1:
        return np.zeros(sample_lats.shape[0], dtype=np.float32)

    points_xy = project_xy_km(sample_lats, sample_lons, context=context)
    k = 2 if source_index is not None and len(node_ids) > 1 else 1
    distances, indices = tree.query(points_xy, k=k)

    if k == 1:
        nearest_km = np.asarray(distances, dtype=np.float32)
    else:
        d = np.asarray(distances, dtype=np.float32)
        i = np.asarray(indices, dtype=np.int32)
        primary_is_source = i[:, 0] == source_index
        nearest_km = np.where(primary_is_source, d[:, 1], d[:, 0]).astype(np.float32)

    penalty = (
        np.maximum(0.0, nearest_km - context.settings.SUPPORT_NEARBY_REPEATER_KM)
        * context.settings.SUPPORT_PENALTY_PER_KM_DB
    )
    return np.clip(penalty, 0.0, context.settings.SUPPORT_MAX_PENALTY_DB).astype(
        np.float32
    )


def current_usable_path_loss_db(*, context):
    return context.rf.calibration.usable_path_loss_db


def current_signal_thresholds_db(*, context):
    return context.rf.calibration.signal_thresholds_db
