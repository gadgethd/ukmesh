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
- coverage polygons

Relevant files:
- `viewshed-worker/rf/config.py`
- `viewshed-worker/rf/loss.py`
- `viewshed-worker/rf/terrain.py`
- `viewshed-worker/worker.py`

## Practical tuning knobs

- `LINK_LOS_MAX_V`
- `DEFAULT_USABLE_PATH_LOSS_DB`
- calibration controls in `rf/config.py`
- frontend display bands in `frontend/src/components/Map/mapConfig.ts`

## Contributor rule

Do not treat packet observation counts as if they are the same thing as physical feasibility. If a change affects topology definition, make sure the evidence class is explicit.
