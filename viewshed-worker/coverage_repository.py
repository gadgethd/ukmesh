"""Coverage repository: explicit worker subsystem ownership."""

from __future__ import annotations
import json
from rf.config import ANTENNA_HEIGHT_M


def already_calculated(
    db, node_id: str, *, require_predicted_links: bool = False, context
) -> bool:
    with db.cursor() as cur:
        cur.execute(
            """SELECT 1
               FROM core.node_coverage nc
               LEFT JOIN core.nodes n ON n.node_id = nc.node_id
               WHERE nc.node_id = %s
                 AND nc.model_version >= %s
                 AND nc.calculation_status IN ('computed', 'permanent')
                 AND (%s IS FALSE OR nc.predicted_links IS NOT NULL OR nc.calculation_status = 'permanent')
                 AND (n.node_id IS NULL OR n.elevation_m IS NOT NULL)""",
            (node_id, context.settings.COVERAGE_MODEL_VERSION, require_predicted_links),
        )
        return cur.fetchone() is not None


def store_coverage(
    db,
    node_id: str,
    geom: dict,
    strength_geoms: dict[str, dict],
    radius_m: float,
    elevation_m: float,
    antenna_height_m: float = ANTENNA_HEIGHT_M,
    *,
    commit: bool = True,
    context,
):
    with db.cursor() as cur:
        cur.execute(
            """INSERT INTO core.node_coverage (
                   node_id, geom, strength_geoms, antenna_height_m, radius_m,
                   model_version, calculation_status, permanent_reason,
                   last_error, retry_after
               )
               VALUES (%s, %s::jsonb, %s::jsonb, %s, %s, %s, 'computed', NULL, NULL, NULL)
               ON CONFLICT (node_id) DO UPDATE
                 SET geom = EXCLUDED.geom,
                     strength_geoms = EXCLUDED.strength_geoms,
                     antenna_height_m = EXCLUDED.antenna_height_m,
                     radius_m = EXCLUDED.radius_m,
                     model_version = EXCLUDED.model_version,
                     calculation_status = 'computed',
                     permanent_reason = NULL,
                     last_error = NULL,
                     retry_after = NULL,
                     calculated_at = NOW()""",
            (
                node_id,
                json.dumps(geom),
                json.dumps(strength_geoms),
                antenna_height_m,
                radius_m,
                context.settings.COVERAGE_MODEL_VERSION,
            ),
        )
        cur.execute(
            "UPDATE core.nodes SET elevation_m = %s WHERE node_id = %s",
            (round(elevation_m, 1), node_id),
        )
    if commit:
        db.commit()


def store_permanent_coverage(
    db,
    node_id: str,
    reason: str,
    *,
    antenna_height_m: float = ANTENNA_HEIGHT_M,
    is_planned: bool = False,
    context,
) -> None:
    safe_reason = str(reason).strip()[:240] or "unsupported"
    with db.cursor() as cur:
        cur.execute(
            """INSERT INTO core.node_coverage (
                   node_id, geom, strength_geoms, antenna_height_m, radius_m,
                   model_version, calculation_status, permanent_reason,
                   last_error, retry_after, is_planned
               )
               VALUES (
                   %s, '{"type":"Polygon","coordinates":[]}'::jsonb, NULL,
                   %s, NULL, %s, 'permanent', %s, NULL, NULL, %s
               )
               ON CONFLICT (node_id) DO UPDATE
                 SET geom = EXCLUDED.geom,
                     strength_geoms = NULL,
                     antenna_height_m = EXCLUDED.antenna_height_m,
                     radius_m = NULL,
                     model_version = EXCLUDED.model_version,
                     calculation_status = 'permanent',
                     permanent_reason = EXCLUDED.permanent_reason,
                     last_error = NULL,
                     retry_after = NULL,
                     is_planned = EXCLUDED.is_planned,
                     calculated_at = NOW()""",
            (
                node_id,
                antenna_height_m,
                context.settings.COVERAGE_MODEL_VERSION,
                safe_reason,
                is_planned,
            ),
        )
    db.commit()
