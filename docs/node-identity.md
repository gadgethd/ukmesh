# Canonical node identity

Why one physical device can appear as multiple nodes, and how the stack
merges them at the data layer (not in the UI).

## The problem

`nodes` is keyed by the exact 64-hex `node_id`. Two failure modes create
ghost rows / "repeater not showing data":

1. A device that re-keys (or advertises a one-nibble-corrupted key) becomes a
   separate row — same name, near-identical keys (GNOME-MSG-RPT ×3,
   2E0MTU RPT Hilperton ×2).
2. MQTT status rows carry the OBSERVER key while advert/position rows carry
   the node's own key — one device's status and position live under
   DIFFERENT rows.

Every flow (history, adverts, owner dashboard, links, aggregates) queries
exact IDs, so data appears split/stale and the owner dashboard lists
repeaters multiple times.

## The fix

- `backend/src/db/nodeIdentity.ts` + migrations **036/037**: evidence-based
  canonical identity merging at the repository layer. Canonical views return
  one row per device with `identity_source_ids` (the merged member keys).
- Perf: canonical views are wrapped in a materialized CTE (migration 037);
  `/api/node-status/latest` resolves canonical members first, then reads per
  member via the `(node_id, time)` index. A naive canonical projection over
  the 5.6M-row telemetry view was >8s; the fixed path is ~1.5s.
- Owner dashboard: the owner API exposes `canonicalId` + `members` for
  authorized keys; the UI collapses same-name identities into one entry with
  the member key list.

## False-merge guard (hard-won)

- Active-to-active packet-pairing alone is NOT identity proof — colocated
  families named `Dunston-1/2/3`, `NorthMesh RPT-1/2`, `Reach Yagi E/NWW`
  must stay separate.
- Differing ordinal/directional suffixes = hard ambiguity, never merge.
- Metadata-only observer rows DO qualify for merging.

## Diagnose

```sql
SELECT name, public_key, last_seen, advert_count
FROM nodes WHERE name ILIKE '<name>';
```
