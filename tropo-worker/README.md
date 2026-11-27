# UKMesh tropo forecast worker

Fetches NOAA/NCEP GFS 0.25° pressure-level fields for the UK, computes a
low-level modified-refractivity (M) gradient, and writes one transparent PNG
per 3-hour forecast step plus `index.json`.

Run:

```bash
python3 -m venv .venv
.venv/bin/pip install -r tropo-worker/requirements.txt
.venv/bin/python tropo-worker/tropo_worker.py --out tropo-data
```

The output directory is served at `/tropo/data/` by the website container; in
production it is bind-mounted read-only into the website container.

The index is 0–150:

- `0` — standard atmosphere (`dM/dz >= 110 N/km`)
- `100` — physical ducting threshold (`dM/dz <= 0`)
- `150` — very strong duct (`dM/dz <= -55 N/km`)

Headline `max_index`, `peak_duct`, `pct_strong` and `pct_ducting` are computed
over UK land grid cells only, using the bundled Natural Earth 1:50m United
Kingdom boundary (`uk-boundary.geojson`, public domain). The PNG shading still
covers the full forecast area, including surrounding waters. GFS rows run
south-to-north; PNGs are flipped north-up for the map overlay.

GFS data is public domain under NOAA's NODD terms. Attribution is requested
and NOAA endorsement must not be implied. Natural Earth is public domain.
