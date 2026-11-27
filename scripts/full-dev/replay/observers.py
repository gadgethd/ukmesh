"""Seed approximate receive areas for the synthetic archive observers."""

from collections import Counter
import csv
from math import isfinite

from prepare import IATA, KEY, observer, rows


def archive_observer_areas(archive, airport_file):
    with airport_file.open(newline='', encoding='utf-8') as source:
        airport_areas = {
            row['iata_code']: (round(float(row['latitude_deg']), 1), round(float(row['longitude_deg']), 1))
            for row in csv.DictReader(source)
            if row['iso_country'] == 'GB' and IATA.fullmatch(row['iata_code'])
            and row['latitude_deg'] and row['longitude_deg']
        }
    receptions = Counter()
    for row in rows(archive, 'packets'):
        key, area = row['rx_node_id'], row['iata']
        if row['network'] == 'ukmesh' and KEY.fullmatch(key) and area in airport_areas:
            receptions[key.upper(), area] += 1

    estimates = {}
    for (key, area), _ in receptions.most_common():
        lat, lon = airport_areas[area]
        if isfinite(lat) and isfinite(lon) and 49 <= lat <= 61 and -9 <= lon <= 3:
            estimates.setdefault(observer(key), (area, lat, lon))
    return estimates


def seed_archive_observers(archive, airport_file, execute):
    estimates = archive_observer_areas(archive, airport_file)
    if not estimates:
        raise ValueError('archive has no usable observer areas')
    values = ',\n'.join(
        f"('{key}', 'Replay observer {area} (area estimate)', '{area}', {lat}, {lon})"
        for key, (area, lat, lon) in estimates.items()
    )
    # Keys are fixed-length hashes and area codes and coordinates were validated above.
    # Leave every existing location and every non-replay identity alone.
    return execute('ukmesh-sample-db', 'meshcore', 'meshcore_ingest', f"""
WITH estimates(node_id, name, iata, lat, lon) AS (VALUES {values}),
     seeded AS (
         INSERT INTO core.nodes (node_id, name, iata, lat, lon, public_key, network)
         SELECT node_id, name, iata, lat, lon, node_id, 'ukmesh' FROM estimates
         ON CONFLICT (node_id) DO UPDATE SET
            lat = EXCLUDED.lat, lon = EXCLUDED.lon, name = EXCLUDED.name
         WHERE nodes.network = 'ukmesh'
           AND (nodes.public_key IS NULL OR nodes.public_key = EXCLUDED.public_key)
           AND nodes.lat IS NULL AND nodes.lon IS NULL
           AND (nodes.name IS NULL OR nodes.name = '' OR nodes.name LIKE 'Archive observer %')
         RETURNING 1
     )
SELECT count(*) FROM seeded;
""")
