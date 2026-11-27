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

GFS data is public domain under NOAA's NODD terms. Attribution is requested
and NOAA endorsement must not be implied.
