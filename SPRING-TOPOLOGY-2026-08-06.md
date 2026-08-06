# UKMesh spring-clean item 11 — topology map

Date: 2026-08-06
Status: implemented, tested, deployed locally on the Projects VPS; not pushed.

## What changed

`TopologyPage.tsx` was graph-only: it used a D3 force layout inside an SVG and
did not render the existing MapLibre map. It now uses a MapLibre-backed UK map
with live repeater nodes and observed relationships overlaid as GeoJSON. Nodes
carry degree, bridge, isolated, and selection state; links carry observation
weight, viability colour, and multibyte evidence styling. The existing region
and multibyte filters remain live, and the side panel still ranks the most
connected repeaters.

The `/api/topology` repository query was also bounded at the base
`node_links` table before canonical aggregation. The old query materialised the
full `node_identity_links` view before applying the recent/viable filters and
was timing out on the live dataset. The new query returns the same public shape
while completing quickly enough for the page and screenshot checks.

The shared MapLibre load handoff was made available before shared layer setup,
and the topology initial view is stable across React renders. This prevents the
map from being torn down after its ready callback and allows the topology
overlay sources, layers, and navigation control to persist.

## Verification

- Live topology: HTTP 200 for `/api/topology?network=ukmesh&limit=300`; 303
  repeaters, 294 mapped repeaters, 300 observed links, 114 components, 49
  likely bridge repeaters, and 100 recently active isolated repeaters.
- HopReach compatibility endpoint: HTTP 200 from
  `/hopreach/api/nodes`.
- Googlebot Playwright capture of `https://ukmesh.com/topology`: HTTP 200,
  topology API HTTP 200, one MapLibre canvas, zoom control present, live
  topology metadata present, and no loading/error state. Evidence:
  `SPRING-TOPOLOGY-2026-08-06.png`.
- `/api/health`: HTTP 200 with `status: healthy`, no incidents, and all three
  deployed service containers healthy. A transient vacuum-backlog incident
  appeared during the first post-deploy refresh and cleared before final
  verification; no database maintenance or unrelated service changes were
  made for this item.

## Quality gates

- Frontend: `npx tsc --noEmit`, 80 tests, `npm run lint:css`, and
  `npm run build` all passed.
- Backend: `npm run typecheck` passed; the isolated backend run passed all 273
  tests. One concurrent run exposed two timing-sensitive failures, which did
  not reproduce in the isolated rerun.

## Deployment

Both frontend bundles contained the new `Live topology` string in their
shipped `TopologyPage` assets. Only `backend`, `app-ukmesh`, and
`website-ukmesh` were deployed; no dependent services were restarted.

- `meshcore-analytics-app:spring-topo` —
  `sha256:15c5acf7ad203be5a95f319becb403c06902bf8bfd8bd540639828e4b6326e9c`
- `meshcore-analytics-website:spring-topo` —
  `sha256:ef4797a250316697294e415f0f8ffc41810f16000ebc713aa77a78d52ba4b0aa`
- `meshcore-analytics-backend:spring-topo` —
  `sha256:983f4e29cb2ac23ee06e35dba5507e590f1f11179170fe46ef90af84bd64ca08`

`.env` was updated only on the `APP_IMAGE` and `WEBSITE_IMAGE` lines. The
backend pin, channel secrets, and every other environment line were preserved.

## Commits

- `be0a689` — `feat(topology): render live repeater links on UK map`
- `7b6e860` — `fix(topology): keep map instance stable after load`

Both commits are local only; nothing was pushed.
