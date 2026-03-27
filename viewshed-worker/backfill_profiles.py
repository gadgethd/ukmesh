#!/usr/bin/env python3
"""
One-time backfill: compute terrain_profile_json for all node_links that are
missing it but already have itm_path_loss_db computed.
"""
import json
import logging
import os
import sys
import tempfile
from pathlib import Path

import psycopg2

# Add the worker source to path
sys.path.insert(0, os.path.dirname(__file__))
from rf.terrain import build_link_vrt, sample_elevation
from rf.loss import compute_path_loss

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(message)s',
                    datefmt='%H:%M:%S')
log = logging.getLogger(__name__)

DB_URL = os.environ.get('DATABASE_URL', 'postgresql://meshcore:meshcore@timescaledb:5432/meshcore')
SRTM_DIR = Path(os.environ.get('SRTM_DIR', '/data/srtm'))


def main():
    db = psycopg2.connect(DB_URL)
    db.autocommit = False

    with db.cursor() as cur:
        cur.execute('''
            SELECT
                nl.node_a_id, nl.node_b_id,
                na.lat AS lat_a, na.lon AS lon_a, na.elevation_m AS elev_a,
                nb.lat AS lat_b, nb.lon AS lon_b, nb.elevation_m AS elev_b
            FROM node_links nl
            JOIN nodes na ON na.node_id = nl.node_a_id
            JOIN nodes nb ON nb.node_id = nl.node_b_id
            WHERE nl.terrain_profile_json IS NULL
              AND nl.itm_computed_at IS NOT NULL
              AND na.lat IS NOT NULL AND na.lon IS NOT NULL
              AND nb.lat IS NOT NULL AND nb.lon IS NOT NULL
        ''')
        rows = cur.fetchall()

    log.info(f'Found {len(rows)} links needing terrain profile backfill')

    ok = 0
    skip = 0
    for i, (a_id, b_id, lat_a, lon_a, elev_a, lat_b, lon_b, elev_b) in enumerate(rows):
        if i % 50 == 0:
            log.info(f'Progress: {i}/{len(rows)} (ok={ok}, skip={skip})')
        try:
            with tempfile.TemporaryDirectory() as tmp:
                vrt = build_link_vrt(lat_a, lon_a, lat_b, lon_b, tmp, SRTM_DIR)
                if not vrt:
                    skip += 1
                    continue
                ea = elev_a if elev_a is not None else sample_elevation(vrt, lat_a, lon_a)
                eb = elev_b if elev_b is not None else sample_elevation(vrt, lat_b, lon_b)
                _, _, profile = compute_path_loss(lat_a, lon_a, ea, lat_b, lon_b, eb, vrt)
                if not profile:
                    skip += 1
                    continue
                with db.cursor() as cur:
                    cur.execute(
                        'UPDATE node_links SET terrain_profile_json = %s WHERE node_a_id = %s AND node_b_id = %s',
                        (json.dumps(profile), a_id, b_id),
                    )
                db.commit()
                ok += 1
        except Exception as exc:
            db.rollback()
            log.warning(f'Failed {a_id[:8]}↔{b_id[:8]}: {exc}')
            skip += 1

    log.info(f'Done. ok={ok}, skip={skip}')
    db.close()


if __name__ == '__main__':
    main()
