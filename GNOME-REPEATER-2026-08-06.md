# GNOME repeater identity investigation and fix

Date: 2026-08-06
Branch: `main`
Scope: backend data layer only; no frontend files or owner-dashboard UI were changed.

## Finding

The previous architecture finding is confirmed in both source and the live database.

- `backend/src/mqtt/client.ts:546-590` uses the exact public key in the MQTT topic as the status observer identity. A status envelope is persisted with that observer key.
- `backend/src/mqtt/client.ts:681-752` decodes an advert sender public key and upserts/increments that decoded sender row. Packet persistence at `:829-852` keeps the topic observer as `rx_node_id` and the decoded sender as `src_node_id`.
- `backend/src/db/index.ts:323-334` increments one exact `nodes.node_id`; the table remains keyed by the exact 64-hex ID. The raw packet, status, link, and sighting tables likewise retain exact IDs.

That produced the expected live split:

| Device | Status/observer identity | Advert/position identity | Live evidence |
| --- | --- | --- | --- |
| GNOME-MSG-RPT | `230F10D0AE10E1F3FC9DBA1B32C48A242EEDAAAF89D440934AEB1D85FD719E5E` | `D7696E95DDE9A236C90322C3B7AEA756F91005E7B2A084D2F1EFDAD207F344A4` | Observer row has status but no position/adverts; advert row is positioned and had 2,736 adverts at investigation time. |
| GNOME-MSG-RPT Lanops.co | — | `D7696E95DDE9A236C90322C3B7AEA756F91605E7B2A084D2F1EFDAD207F344A4` | Same position as the active advert row and differs from it by one key nibble; one stale/duplicate advert row. |
| GNOME-STKTN-RPT | `6E9F3F33EE2E955BE8BBDDF7EC043C7B5B4A8CFD13E53456A382124B75E29F3E` | `65821B80039F42EEDA42C693D503873E3CCF2E83D1DAB85FB94A4433CBA104F8` | Observer status plus direct observer-to-advert packet evidence; V4 advert row is positioned. |
| GNOME-STKTN-RPT (stale) | `ADAE43BE6BDA85D606A842E755037B8319935F3B019B50D9924CE70AFB3E64E` | — | Same identity name, stale metadata-only row. |
| GNOME-MOBILE-RPT | `CEF1D1C2D5519A67680EF97C3A13BE3C1432AA201CFE4785A7F2E7E33A51B0F8` | same raw row | Stale and has no independent pairing evidence; intentionally remains separate. |

Before this change, public projections queried those exact rows independently, so the status key and advert key could not produce one repeater view.

## General identity model

Migration `backend/src/db/migrations/036_node_identity_merge.sql` adds:

- `node_identity_aliases`: reversible `source_node_id -> canonical_node_id` mappings, confidence, reason, and JSON evidence. Raw rows are never deleted or rewritten.
- `node_identity_match_evidence`: accepted and ambiguous candidate pairs, including score, confidence, reason, and evidence. Ambiguous pairs are retained for review but do not become aliases.
- `meshcore_canonical_node_id(text)`: a stable SQL resolver used by the data layer.
- Canonical views for nodes, sightings, statuses, packets, links, and link-radio reports.

`node_identity_nodes` emits one row per canonical identity. It selects a positioned repeater representative first, then a repeater row, advert evidence, last-seen time, and key as tie-breakers; it aggregates member liveness, timestamps, and advert counts and exposes `identity_source_ids` and `identity_member_count`. The other views canonicalize IDs while preserving the original telemetry/packet observations. Canonical links are grouped as unordered endpoint pairs and direction counters are rotated when required.

The backend refreshes automatic evidence at startup and every 30 minutes. Existing automatic aliases are replaced atomically inside a transaction; manual aliases, if added later, are preserved by `source_kind`.

## Merge rule and confidence policy

The rule is deliberately evidence-based and applies to all public repeaters, not to a GNOME-specific name list.

1. Names are normalized with Unicode NFKC, upper-cased, punctuation-separated, `RPT` normalized to `REPEATER`, and version suffixes such as `V4` removed. Candidates must be in the same public network family, have compatible repeater roles, and share a normalized name bucket. Name similarity alone never merges anything.
2. A high-confidence pair is accepted when one of these independent signals exists in addition to the common name/network/role checks:
   - keys differ in at most one hexadecimal nibble and valid positions are within 3 km;
   - there are at least three packet observations and three decoded adverts directly pairing a metadata-only observer row with an active positioned advert row, with status evidence on the pair; or
   - both keys have at least three self-adverts, their activity intervals do not overlap, the handover gap is at most 14 days, and status plus an active positioned repeater support the handover.
3. A medium-confidence pair is a metadata-only status row (no role, valid position, or adverts) plus exactly one unique active positioned repeater with the same normalized name/network and status evidence. Near-key duplicate active rows are collapsed when establishing that uniqueness.
4. A shared base name with a changed final ordinal/directional token is a veto for the weaker evidence paths. This protects genuinely separate colocated devices such as `Dunston-1/2`, `NorthMesh RPT-1/2`, and `Reach Yagi E/NWW`. Active-active packet hearing is not treated as identity evidence: one repeater hearing another is normal mesh behavior. The one-nibble-key-plus-position rule is the explicit strong-signal exception to the name-variant veto.
5. A group may not contain positioned members more than 3 km apart. Test-network rows are excluded from automatic grouping.

The resulting canonical key is the best active positioned/advert-backed member, with deterministic last-seen and key tie-breakers. Stale and duplicate rows are therefore hidden from canonical public projections, not destroyed. Raw tables remain available for audit and rollback, while ambiguous candidates remain visible in `node_identity_match_evidence` rather than being silently merged.

## Data-layer wiring

The canonical projections are used by the public node/map/repeater APIs, status latest/history, packet history/adverts/recent packet/event projections, network filters, viable links and radio reports, topology/network analysis, stats, exports, HopReach compatibility, and owner live-data repository queries. The owner-dashboard UI itself was not changed. The latest status route resolves canonical members first and uses the existing per-node/time telemetry index, so the new merge does not turn the status endpoint into an unbounded full-view scan.

## Live result after deployment

The live canonical view now reports:

| Canonical name | Canonical ID | Members | Source IDs | Position | Advert count |
| --- | --- | ---: | --- | --- | ---: |
| GNOME-MSG-RPT | `D7696E95DDE9A236C90322C3B7AEA756F91005E7B2A084D2F1EFDAD207F344A4` | 3 | `230F…195E`, active `D769…F344A4`, Lanops `D769…F916…F344A4` | 54.521892, -1.473446 | 2,738 |
| GNOME-STKTN-RPT-V4 | `65821B80039F42EEDA42C693D503873E3CCF2E83D1DAB85FB94A4433CBA104F8` | 3 | active `6582…A104F8`, `6E9F…9F3E`, stale `ADAE…E64E` | 54.562317, -1.366457 | 352 |
| GNOME-MOBILE-RPT | `CEF1D1C2D5519A67680EF97C3A13BE3C1432AA201CFE4785A7F2E7E33A51B0F8` | 1 | itself | no valid position (0,0) | 42 |

The live API checks returned:

- `/api/nodes?network=ukmesh`: HTTP 200; one positioned row for each active GNOME repeater, plus the intentionally separate stale mobile row.
- `/api/node-status/latest?network=ukmesh`: HTTP 200; GNOME status rows are returned under the canonical advert IDs. The MSG row had a current status timestamp and uptime; STKTN likewise returned under `65821…A104F8`.
- `/api/node-status/history?...nodeId=D7696E95...`: HTTP 200; 2,885 merged MSG telemetry points over 24 hours.
- `/hopreach/healthz`: HTTP 200, `{"status":"ok"}`.
- `/api/health`: HTTP 200, `{"status":"healthy","incidents":[]}` after the normal post-restart health snapshot refresh; the backend container health probe is healthy.

The refresh currently records 20 accepted automatic pairs and 15,342 ambiguous same-name candidates. The false-merge regression checks for the colocated Dunston, NorthMesh, Reach Yagi, and NE33 families found zero aliases. This is the intended behavior: high-signal identities merge, uncertain same-name candidates remain separate and auditable.

## Deployment and verification

- Migration 036 was applied to the live database by the backend migration runner.
- Backend image was built with `BACKEND_IMAGE=meshcore-analytics-backend:gnome-fix` and deployed as digest `sha256:03e54021c9ea0ebcd6febcfec28c5b67fd222af28cb3c5c56a3589ab316cf7ca`. The host `.env` pin was updated to that digest.
- Only the backend was recreated with `docker compose -f docker-compose.yml -f docker-compose.live.yml up -d --no-deps --force-recreate backend`. Mosquitto, TimescaleDB, Redis, HopReach, workers, Discord bot, and beacon services were not restarted or changed.
- `npm run typecheck` passed. The six focused node-identity tests passed. The full backend suite passed 269 of 270 tests; the one failure is the existing unrelated cache-policy assertion for `src/mqtt/channelRegistry.ts#channelCache`.

No screenshot was taken because the frontend was not changed; the frontend-visible data is verified through the same node/status APIs it consumes.
