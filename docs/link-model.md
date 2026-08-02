# Link Model

## Purpose

`node_links` represents the physical or physically-rated link graph used by the map and the path resolver.

## Evidence layers

The model distinguishes between:

- physical feasibility
  - terrain
  - path loss
  - LOS tolerance
- radio-neighbour evidence
  - repeater-reported neighbour quality
- packet evidence
  - strongest: multibyte path evidence
  - weaker: generic packet observation

## Worker ownership

The worker computes:
- terrain profile
- total path loss
- viability

Relevant files:
- `viewshed-worker/rf/config.py`
- `viewshed-worker/rf/loss.py`
- `viewshed-worker/rf/terrain.py`
- `viewshed-worker/worker.py` in `WORKER_MODE=link`

## Practical tuning knobs

- `LINK_LOS_MAX_V`
- `DEFAULT_USABLE_PATH_LOSS_DB`
- calibration controls in `rf/config.py`
- `RF_PREFIX_ENDPOINT_BATCH` and `RF_PREFIX_RAY_BATCH` for bounded CPU/memory tuning
- frontend display bands in `frontend/src/components/Map/mapConfig.ts`

## Contributor rule

Do not treat packet observation counts as if they are the same thing as physical feasibility. If a change affects topology definition, make sure the evidence class is explicit.

Whole-region RF coverage is a separate HopReach raster pipeline and does not
consume the link worker's former viewshed polygons. Its capacity evidence,
source boundary, and rollout gate are in `docs/rf-coverage-rollout.md`.
