"""Link model: explicit worker subsystem ownership."""

from __future__ import annotations
from rf_context import current_usable_path_loss_db
import json
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional
from rf.config import ANTENNA_HEIGHT_M
from rf.loss import compute_path_loss
from rf.terrain import (
    InvalidTerrainRequest,
    PermanentOutOfScope,
    RetryableTerrainError,
    build_vrt,
    build_link_vrt,
    ensure_tiles_for_link,
    sample_elevation,
)
from link_repository import upsert_link_pair
from link_topology import link_pair_key
from rf_context import get_support_tree
from worker_math import (
    check_job_budget,
    node_dist_km,
    physical_candidate_radius_km,
    project_xy_km,
)
import logging

log = logging.getLogger("worker")


def compute_planned_links(
    db,
    lat: float,
    lon: float,
    elevation_m: float,
    radius_m: Optional[float],
    antenna_height_m: float = ANTENNA_HEIGHT_M,
    *,
    stage_seconds: Optional[dict[str, float]] = None,
    deadline_monotonic: Optional[float] = None,
    cancellation_check: Optional[Callable[[], None]] = None,
    context,
) -> list[dict]:
    """Predict viable RF links from a hypothetical repeater at (lat, lon) to nearby
    real positioned repeaters, using the same ITM path-loss model as physical links.

    Returns a list of {peer_id, peer_name, itm_path_loss_db, itm_viable, distance_km}
    for peers the planned repeater would reach, ordered best (lowest loss) first.
    """
    check_job_budget(deadline_monotonic, cancellation_check, context=context)
    timings = stage_seconds if stage_seconds is not None else {}
    origin = {"lat": lat, "lon": lon, "radius_m": radius_m, "elevation_m": elevation_m}
    origin_radius_km = physical_candidate_radius_km(radius_m, context=context)

    peer_lookup_started = time.monotonic()
    all_nodes, _physical_pairs = context.topology.get(db)
    support_tree = get_support_tree(context=context)
    support_node_ids: list[str] = context.rf.support.node_ids
    if support_tree is not None and support_node_ids:
        origin_xy = project_xy_km([lat], [lon], context=context)[0]
        nearby_indices = support_tree.query_ball_point(
            origin_xy,
            # The tree uses one UK reference latitude. Over-query so its
            # equirectangular longitude scale cannot exclude a true candidate;
            # node_dist_km applies the exact bounded filter below.
            r=context.settings.MAX_PHYSICAL_LINK_RADIUS_KM * 1.35,
        )
        candidate_ids = [
            support_node_ids[int(index)]
            for index in nearby_indices
            if int(index) < len(support_node_ids)
        ]
    else:
        candidate_ids = list(all_nodes)

    candidates: list[tuple[float, str, dict]] = []
    for peer_id in candidate_ids:
        peer = all_nodes.get(peer_id)
        if peer is None:
            continue
        if peer.get("lat") is None or peer.get("lon") is None:
            continue
        dist_km = node_dist_km(origin, peer)
        reach_km = max(
            origin_radius_km,
            physical_candidate_radius_km(peer.get("radius_m"), context=context),
        )
        if dist_km > reach_km:
            continue
        candidates.append((dist_km, peer_id, peer))
    candidates.sort(key=lambda c: c[0])
    candidates = candidates[: context.settings.MAX_PLANNED_LINK_PEERS]
    timings["planned_peer_lookup"] = round(
        time.monotonic() - peer_lookup_started,
        6,
    )
    check_job_budget(deadline_monotonic, cancellation_check, context=context)
    if not candidates:
        return []

    links: list[dict] = []
    with tempfile.TemporaryDirectory() as tmp:
        tile_started = time.monotonic()
        terrain_paths: dict[str, Path] = {}
        terrain_candidates: list[tuple[float, str, dict]] = []
        for dist_km, peer_id, peer in candidates:
            check_job_budget(deadline_monotonic, cancellation_check, context=context)
            try:
                paths = ensure_tiles_for_link(
                    context.settings.SRTM_DIR,
                    lat,
                    lon,
                    peer["lat"],
                    peer["lon"],
                    log,
                )
                for path in paths:
                    terrain_paths[str(path)] = path
                terrain_candidates.append((dist_km, peer_id, peer))
            except PermanentOutOfScope as exc:
                log.info(f"Planned link {peer_id[:8]}… is outside terrain scope: {exc}")
        timings["planned_tile_acquisition"] = round(
            time.monotonic() - tile_started,
            6,
        )
        if not terrain_candidates:
            return []

        check_job_budget(deadline_monotonic, cancellation_check, context=context)
        vrt_started = time.monotonic()
        vrt = f"{tmp}/planned-links.vrt"
        build_vrt(terrain_paths.values(), vrt)
        timings["planned_vrt_construction"] = round(
            time.monotonic() - vrt_started,
            6,
        )

        link_started = time.monotonic()
        for dist_km, peer_id, peer in terrain_candidates:
            check_job_budget(deadline_monotonic, cancellation_check, context=context)
            try:
                peer_elev = peer.get("elevation_m")
                if peer_elev is None:
                    peer_elev = sample_elevation(vrt, peer["lat"], peer["lon"])
                result = compute_path_loss(
                    lat,
                    lon,
                    elevation_m,
                    peer["lat"],
                    peer["lon"],
                    peer_elev,
                    vrt,
                    antenna_height_m_tx=antenna_height_m,
                    antenna_height_m_rx=peer.get("antenna_height_m", ANTENNA_HEIGHT_M),
                    usable_threshold_db=current_usable_path_loss_db(context=context),
                )
                if not result.viable:
                    continue
                links.append(
                    {
                        "peer_id": peer_id,
                        "peer_name": peer.get("name"),
                        "itm_path_loss_db": round(float(result.path_loss_db), 1),
                        "itm_viable": True,
                        "distance_km": round(dist_km, 1),
                    }
                )
            except Exception as exc:
                raise RetryableTerrainError(
                    f"planned link {peer_id[:8]} path loss failed: {exc}"
                ) from exc
        timings["planned_per_link_calculation"] = round(
            time.monotonic() - link_started,
            6,
        )
    check_job_budget(deadline_monotonic, cancellation_check, context=context)
    links.sort(key=lambda link: link["itm_path_loss_db"])
    return links


def publish_link_update(
    r_client,
    a_id: str,
    b_id: str,
    obs_count: int,
    path_loss_db: Optional[float],
    itm_viable: Optional[bool],
    count_a_to_b: int,
    count_b_to_a: int,
    multibyte_obs: int,
    *,
    context,
) -> None:
    r_client.publish(
        context.settings.LIVE_CHANNEL,
        json.dumps(
            {
                "type": "link_update",
                "data": {
                    "node_a_id": a_id,
                    "node_b_id": b_id,
                    "observed_count": obs_count,
                    "itm_path_loss_db": path_loss_db,
                    "itm_viable": itm_viable,
                    "count_a_to_b": count_a_to_b,
                    "count_b_to_a": count_b_to_a,
                    "multibyte_observed_count": multibyte_obs,
                },
                "ts": int(time.time() * 1000),
            }
        ),
    )


def ensure_physical_link_metrics(
    db, a_id: str, a: dict, b_id: str, b: dict, topology=None, *, context
):
    # Existing physical-pair jobs are read-only until a metric is actually
    # missing. Avoid the old zero-delta ON CONFLICT UPDATE, while retaining a
    # race-safe insert-and-reread path for a genuinely absent pair.
    if topology is None:
        topology = context.topology
    select_pair_sql = """SELECT observed_count, itm_computed_at, itm_path_loss_db, itm_viable,
                                count_a_to_b, count_b_to_a, multibyte_observed_count
                         FROM {node_links}
                         WHERE node_a_id = %s AND node_b_id = %s"""
    with db.cursor() as cur:
        topology.graph.execute(cur, select_pair_sql, (a_id, b_id))
        row = cur.fetchone()
        if row is None:
            topology.graph.execute(
                cur,
                """INSERT INTO {node_links}
                     (node_a_id, node_b_id, observed_count, last_observed,
                      count_a_to_b, count_b_to_a, multibyte_observed_count)
                   VALUES (%s, %s, 0, NOW(), 0, 0, 0)
                   ON CONFLICT (node_a_id, node_b_id) DO NOTHING
                   RETURNING observed_count, itm_computed_at, itm_path_loss_db, itm_viable,
                             count_a_to_b, count_b_to_a, multibyte_observed_count""",
                (a_id, b_id),
            )
            row = cur.fetchone()
            if row is None:
                topology.graph.execute(cur, select_pair_sql, (a_id, b_id))
                row = cur.fetchone()
    obs_count = row[0] if row else 0
    itm_computed = row[1] if row else None
    path_loss_db = row[2] if row else None
    itm_viable = row[3] if row else None
    count_a_to_b = row[4] if row else 0
    count_b_to_a = row[5] if row else 0
    multibyte_obs = row[6] if row else 0

    a = dict(a)
    b = dict(b)
    missing_endpoint_elev = a.get("elevation_m") is None or b.get("elevation_m") is None
    if itm_computed is not None and not missing_endpoint_elev:
        return (
            obs_count,
            path_loss_db,
            itm_viable,
            count_a_to_b,
            count_b_to_a,
            multibyte_obs,
        )

    with tempfile.TemporaryDirectory() as tmp:
        ensure_tiles_for_link(
            context.settings.SRTM_DIR,
            a["lat"],
            a["lon"],
            b["lat"],
            b["lon"],
            log,
        )
        vrt = build_link_vrt(
            a["lat"], a["lon"], b["lat"], b["lon"], tmp, context.settings.SRTM_DIR
        )
        try:
            a_elev = a.get("elevation_m")
            b_elev = b.get("elevation_m")
            if a_elev is None:
                a_elev = sample_elevation(vrt, a["lat"], a["lon"])
                a["elevation_m"] = a_elev
                topology.update_node(a_id, a)
                with db.cursor() as cur:
                    cur.execute(
                        "UPDATE core.nodes SET elevation_m = %s WHERE node_id = %s AND elevation_m IS NULL",
                        (round(a_elev, 1), a_id),
                    )
            if b_elev is None:
                b_elev = sample_elevation(vrt, b["lat"], b["lon"])
                b["elevation_m"] = b_elev
                topology.update_node(b_id, b)
                with db.cursor() as cur:
                    cur.execute(
                        "UPDATE core.nodes SET elevation_m = %s WHERE node_id = %s AND elevation_m IS NULL",
                        (round(b_elev, 1), b_id),
                    )
            result = compute_path_loss(
                a["lat"],
                a["lon"],
                a_elev,
                b["lat"],
                b["lon"],
                b_elev,
                vrt,
                antenna_height_m_tx=a.get("antenna_height_m", ANTENNA_HEIGHT_M),
                antenna_height_m_rx=b.get("antenna_height_m", ANTENNA_HEIGHT_M),
                usable_threshold_db=current_usable_path_loss_db(context=context),
            )
            path_loss_db = round(result.path_loss_db, 1)
            itm_viable = result.viable
            with db.cursor() as cur:
                topology.graph.execute(
                    cur,
                    """UPDATE {node_links}
                       SET itm_path_loss_db = %s,
                           itm_viable = %s,
                           itm_computed_at = NOW(),
                           terrain_profile_json = %s::jsonb
                       WHERE node_a_id = %s AND node_b_id = %s""",
                    (path_loss_db, itm_viable, json.dumps(result.profile), a_id, b_id),
                )
            log.info(
                f"Link {a_id[:8]}…↔{b_id[:8]}…: "
                f"{path_loss_db:.1f} dB {'✓' if itm_viable else '✗'} "
                f"(obs={obs_count})"
            )
        except (PermanentOutOfScope, InvalidTerrainRequest):
            raise
        except Exception as exc:
            raise RetryableTerrainError(f"path loss computation failed: {exc}") from exc

    return (
        obs_count,
        path_loss_db,
        itm_viable,
        count_a_to_b,
        count_b_to_a,
        multibyte_obs,
    )


def process_physical_link_job(db, r_client, job: dict, topology=None, *, context):
    if topology is None:
        topology = context.topology
    node_a_id = job.get("node_a_id")
    node_b_id = job.get("node_b_id")
    if not node_a_id or not node_b_id or node_a_id == node_b_id:
        return

    nodes, physical_pairs = topology.get(db, (node_a_id, node_b_id))
    a = nodes.get(node_a_id)
    b = nodes.get(node_b_id)
    if not a or not b:
        return

    obs_count, path_loss_db, itm_viable, count_a_to_b, count_b_to_a, multibyte_obs = (
        ensure_physical_link_metrics(
            db, node_a_id, a, node_b_id, b, topology, context=context
        )
    )
    if itm_viable:
        topology.include_pair(node_a_id, node_b_id)
    if topology.scope == "public":
        publish_link_update(
            r_client,
            node_a_id,
            node_b_id,
            obs_count,
            path_loss_db,
            itm_viable,
            count_a_to_b,
            count_b_to_a,
            multibyte_obs,
            context=context,
        )


def process_observation_link_job(db, r_client, job: dict, topology=None, *, context):
    """Resolve multibyte packet paths and annotate links.

    When both endpoints of a hop are uniquely resolved (exactly one node
    matched the hash prefix), the link is upserted even if it has no prior
    physical-link entry — multibyte evidence is precise enough to bootstrap
    new links directly.  Ambiguously-resolved hops retain the conservative
    behaviour of only annotating pre-existing physical links.
    """
    if topology is None:
        topology = context.topology
    rx_node_id = job.get("rx_node_id")
    src_node_id = job.get("src_node_id")
    path_hashes = job.get("path_hashes", [])
    path_hash_size_bytes = int(job.get("path_hash_size_bytes") or 1)

    if not rx_node_id or not path_hashes or path_hash_size_bytes <= 1:
        return

    all_nodes, physical_pairs = topology.get(db, (rx_node_id,))

    def physical_link(a_id: str, b_id: str) -> bool:
        return link_pair_key(a_id, b_id) in topology.snapshot.physical_pairs

    rx = all_nodes.get(rx_node_id)
    if not rx:
        return

    def normalize_path_hash(value) -> str:
        return str(value or "").strip().upper()

    def node_matches_path_hash(node_id: str, path_hash: str) -> bool:
        return bool(path_hash) and node_id.upper().startswith(path_hash)

    def local_prefix_ambiguity_penalty(
        path_hash: str,
        target_id: str,
        target_node: dict,
        anchor_node: dict,
        pool: list[tuple[str, dict]],
    ) -> float:
        target_dist = node_dist_km(target_node, anchor_node)
        raw = 0.0
        for cand_id, cand_node in pool:
            if cand_id == target_id:
                continue
            if not node_matches_path_hash(cand_id, path_hash):
                continue
            cand_dist = node_dist_km(cand_node, anchor_node)
            if cand_dist > context.settings.PREFIX_AMBIGUITY_RADIUS_KM:
                continue
            dist_similarity = max(
                0.0,
                1.0
                - abs(cand_dist - target_dist)
                / context.settings.PREFIX_AMBIGUITY_RADIUS_KM,
            )
            proximity = max(
                0.0, 1.0 - cand_dist / context.settings.PREFIX_AMBIGUITY_RADIUS_KM
            )
            raw += dist_similarity * proximity
        return min(0.24, raw * 0.12)

    # Each entry: (node_id, node_data, is_unique)
    # is_unique=True means exactly one node matched the hash — high-confidence.
    resolved: list[tuple[str, dict, bool]] = []
    prev_id = rx_node_id
    prev = rx
    visited = {rx_node_id}

    for raw_hash in reversed(path_hashes):
        path_hash = normalize_path_hash(raw_hash)
        if not path_hash:
            continue
        candidates = [
            (nid, nd)
            for nid, nd in all_nodes.items()
            if node_matches_path_hash(nid, path_hash) and nid not in visited
        ]
        if not candidates:
            continue

        is_unique = len(candidates) == 1

        best_id = None
        best = None
        best_score = float("-inf")
        for nid, nd in candidates:
            confirmed_bonus = 2.5 if physical_link(nid, prev_id) else 0.0
            distance_score = -node_dist_km(nd, prev) / 12.0
            ambiguity_penalty = local_prefix_ambiguity_penalty(
                path_hash, nid, nd, prev, candidates
            )
            score = confirmed_bonus + distance_score - ambiguity_penalty
            if score > best_score:
                best_score = score
                best_id, best = nid, nd

        if best_id is None or best is None:
            continue

        resolved.insert(0, (best_id, best, is_unique))
        visited.add(best_id)
        prev_id = best_id
        prev = best

    # rx_node_id is always uniquely known (it's the observer), so mark it unique.
    # src_node_id from the packet header is also authoritative if present.
    full: list[tuple[str, dict, bool]] = []
    if src_node_id and src_node_id in all_nodes:
        full.append((src_node_id, all_nodes[src_node_id], True))
    full.extend(resolved)
    full.append((rx_node_id, rx, True))
    if len(full) < 2:
        return

    for i in range(len(full) - 1):
        src_id, src, src_unique = full[i]
        dst_id, dst, dst_unique = full[i + 1]
        if src_id == dst_id:
            continue
        if (
            src["lat"] is None
            or src["lon"] is None
            or dst["lat"] is None
            or dst["lon"] is None
        ):
            continue

        if src_id < dst_id:
            a_id, a, b_id, b = src_id, src, dst_id, dst
            inc_atob, inc_btoa = 1, 0
        else:
            a_id, a, b_id, b = dst_id, dst, src_id, src
            inc_atob, inc_btoa = 0, 1

        # Allow new link creation only when both endpoints were uniquely resolved.
        # For ambiguous hops, stay conservative and only annotate existing links.
        both_unique = src_unique and dst_unique
        if not both_unique and not physical_link(a_id, b_id):
            continue

        # Keep directional observations, but reserve multibyte evidence for exact
        # endpoint matches so ambiguous two-byte prefixes do not inflate path history.
        row = upsert_link_pair(
            db,
            a_id,
            b_id,
            inc_atob,
            inc_btoa,
            1 if both_unique else 0,
            graph=topology.graph,
        )
        obs_count = row[0] if row else 1
        path_loss_db = row[2] if row else None
        itm_viable = row[3] if row else None
        count_a_to_b = row[4] if row else inc_atob
        count_b_to_a = row[5] if row else inc_btoa
        multibyte_obs = row[6] if row else 1

        if itm_viable is None:
            (
                obs_count,
                path_loss_db,
                itm_viable,
                count_a_to_b,
                count_b_to_a,
                multibyte_obs,
            ) = ensure_physical_link_metrics(
                db, a_id, a, b_id, b, topology, context=context
            )
        if itm_viable:
            topology.include_pair(a_id, b_id)
        if topology.scope == "public":
            publish_link_update(
                r_client,
                a_id,
                b_id,
                obs_count,
                path_loss_db,
                itm_viable,
                count_a_to_b,
                count_b_to_a,
                multibyte_obs,
                context=context,
            )


def process_link_job(db, r_client, job: dict, topology=None, *, context):
    if topology is None:
        topology = context.topology
    job_type = str(job.get("type") or "observe").strip().lower()
    if job_type == "physical_pair":
        process_physical_link_job(db, r_client, job, topology, context=context)
        return
    process_observation_link_job(db, r_client, job, topology, context=context)
