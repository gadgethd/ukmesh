"""Link repository: explicit worker subsystem ownership."""

from __future__ import annotations
import json
from worker_settings import DEFAULT_SETTINGS
import re
from dataclasses import dataclass
from psycopg2 import sql
from rf.config import ANTENNA_HEIGHT_M


def load_positioned_repeaters(db, *, settings=DEFAULT_SETTINGS) -> dict[str, dict]:
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT n.node_id, n.lat, n.lon, n.elevation_m, n.name, n.role, nc.radius_m, nc.antenna_height_m
            FROM core.nodes n
            LEFT JOIN core.node_coverage nc ON nc.node_id = n.node_id
            WHERE n.lat IS NOT NULL
              AND n.lon IS NOT NULL
              AND n.lat BETWEEN %s AND %s
              AND n.lon BETWEEN %s AND %s
              AND NOT (ABS(n.lat) < 1e-9 AND ABS(n.lon) < 1e-9)
              AND (n.name IS NULL OR n.name NOT LIKE %s)
              AND (n.role IS NULL OR n.role = 2)
            """,
            (
                settings.UK_LAT_MIN,
                settings.UK_LAT_MAX,
                settings.UK_LON_MIN,
                settings.UK_LON_MAX,
                "%🚫%",
            ),
        )
        return {
            row[0]: {
                "lat": row[1],
                "lon": row[2],
                "elevation_m": row[3],
                "name": row[4],
                "role": row[5],
                "radius_m": row[6],
                "antenna_height_m": row[7] if row[7] is not None else ANTENNA_HEIGHT_M,
            }
            for row in cur.fetchall()
        }


def store_planned_links(db, node_id: str, links: list[dict]) -> None:
    with db.cursor() as cur:
        cur.execute(
            "UPDATE core.node_coverage SET predicted_links = %s::jsonb WHERE node_id = %s",
            (json.dumps(links), node_id),
        )
    db.commit()


@dataclass(frozen=True)
class GraphRepository:
    schema: str = "core"

    def __post_init__(self):
        if (
            self.schema != "core"
            and re.fullmatch(r"link_rebuild_[0-9a-f]{16}", self.schema) is None
        ):
            raise ValueError("Invalid link rebuild generation")

    def execute(self, cur, statement, params=()):
        cur.execute(
            sql.SQL(statement).format(
                node_links=sql.Identifier(self.schema, "node_links")
            ),
            params,
        )


LIVE_GRAPH = GraphRepository()


def upsert_link_pair(
    db,
    a_id: str,
    b_id: str,
    inc_atob: int,
    inc_btoa: int,
    inc_multibyte: int,
    graph=LIVE_GRAPH,
):
    obs_delta = inc_atob + inc_btoa
    with db.cursor() as cur:
        graph.execute(
            cur,
            """INSERT INTO {node_links}
                   (node_a_id, node_b_id, observed_count, last_observed,
                    count_a_to_b, count_b_to_a, multibyte_observed_count)
               VALUES (%s, %s, %s, NOW(), %s, %s, %s)
               ON CONFLICT (node_a_id, node_b_id) DO UPDATE
                 SET observed_count = node_links.observed_count + %s,
                     last_observed = CASE WHEN %s > 0 THEN NOW() ELSE node_links.last_observed END,
                     count_a_to_b = node_links.count_a_to_b + %s,
                     count_b_to_a = node_links.count_b_to_a + %s,
                     multibyte_observed_count = node_links.multibyte_observed_count + %s
               RETURNING observed_count, itm_computed_at, itm_path_loss_db, itm_viable,
                         count_a_to_b, count_b_to_a, multibyte_observed_count""",
            (
                a_id,
                b_id,
                obs_delta,
                inc_atob,
                inc_btoa,
                inc_multibyte,
                obs_delta,
                obs_delta,
                inc_atob,
                inc_btoa,
                inc_multibyte,
            ),
        )
        return cur.fetchone()
