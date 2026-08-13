# MapLibre 6 terrain regression report

Date: 2026-08-13 UTC
Status: fixed and deployed to `app-ukmesh`, `website-ukmesh`, and `website-dev`

## Root cause and fix

MapLibre GL 6's terrain tile-pyramid selection produced a single raised DEM square when the local `raster-dem` source explicitly declared `minzoom: 5`. Upgrading MapLibre from 6.2.0 to 6.3.0 and correcting `tileSize` from 512 to the tiles' actual 256 pixels were necessary source corrections, but did not resolve the rendering defect by themselves.

The decisive bare-MapLibre test rendered continuous multi-tile terrain with this source specification:

- MapLibre GL 6.3.0
- `tileSize: 256`
- `encoding: "terrarium"`
- `maxzoom: 12`
- no explicit `minzoom`
- terrain exaggeration 3

Removing only `minzoom: 5` from the application source fixed the full application while retaining hillshade, sky/atmosphere, and the deck.gl overlays. MapLibre can now select the required parent/child DEM pyramid levels itself.

## Code changes

Commit `18aed54ba0b4fd451c26ae6dc8fe687e07a51f1a` (`fix(map): restore multi-tile terrain on MapLibre 6`):

- upgraded `maplibre-gl` from 6.2.0 to 6.3.0 in `frontend/package.json` and `frontend/package-lock.json`;
- changed `TERRAIN_DEM_SOURCE.tileSize` from 512 to 256;
- changed the two DEM source error-handler parameter types to `maplibregl.ErrorEvent`, preserving their behavior.

Commit `d87b3d39c1a3f6a9ca742062515a2d316581ff3e` (`fix(map): let MapLibre select terrain DEM pyramid`):

- removed the explicit `TERRAIN_DEM_SOURCE.minzoom` value.

No backend code, CSS, hillshade behavior, sky behavior, or deck.gl behavior changed in the production fix.

## Hypothesis protocol

1. H1, shared hillshade source: removing both hillshade layer paths still failed to show continuous relief. Eliminated.
2. H2, sky/atmosphere: additionally removing `map.setSky(...)` still failed. Eliminated.
3. H3, deck.gl `MapboxOverlay`: additionally omitting both live overlay controllers still failed to provide the required continuous visual result. The diagnostic candidate was not a fixed first-loaded tile: center/candidate changed from `11/1017/652` / `11/1016/651`, to `11/1017/652` / `11/1017/651` after panning, and to `12/2035/1304` / `12/2035/1304` after zooming. Without hillshade the visual delta was weak, so this was not accepted as a fix.
4. H4, bare MapLibre 6.3: the first protocol variant, `tileSize=256`, absent `minzoom`, Terrarium encoding, exaggeration 3, rendered multi-tile terrain. Per the stop-on-success protocol, later variants were not run.

The H4 camera-center tiles changed from `10/508/326` to `10/509/325` on pan and `11/1018/651` on zoom. Terrain-related pixel changes covered 16 distinct z10 tiles and non-null `queryTerrainElevation` samples covered 19 distinct z10 tiles. Therefore the rendered relief tracked the visible pyramid across many tiles; it was neither a center-tile-only bug nor a fixed first-loaded/cache-order tile.

## Frontend gates

Run from `frontend/` against the final source revision:

- `npx tsc --noEmit`: passed cleanly.
- `npm test`: passed, 94/94 tests.
- `npm run build`: passed.
- CSS lint: the repository's known pre-existing duplicate-selector failures remain in untouched CSS (`owner-portal.css` and sibling files). `git diff --name-only` confirmed those files were not part of this change; no unrelated CSS was modified.

## Images and deployment

All images contain MapLibre 6.3.0 in `assets/maplibre-gl-OTXnCXpW.js` and carry source revision `d87b3d39c1a3f6a9ca742062515a2d316581ff3e`.

| Service | Build tag | Pinned image digest |
| --- | --- | --- |
| `app-ukmesh` | `meshcore-analytics-app:v6fix-final` | `sha256:620db704760dbe91f471ce984c9805ec2257a13aa4097bb634beffbe07ccc386` |
| `website-ukmesh` | `meshcore-analytics-website:v6fix-final` | `sha256:2779d7c5da0062e8a2a5d4e2aea10ed66faaf0e492646a677a0eb30fa4a92b18` |
| `website-dev` | `meshcore-analytics-website-dev:v6fix-final` | `sha256:3674270ba54601e33ea693e95235791929f0bfd34b791cdbd016e8904a6c79e3` |

The app was recreated with `up -d --no-deps app-ukmesh`. Both website services were deployed with `scripts/deploy-website.sh --force`; their drift checks, digest pins, recreation, and served-bundle verification passed. No other service was recreated.

The refreshed manifests were captured at 2026-08-13T00:06:18Z. Their follow-up drift checks report 90/90 identical files for production and 76/76 identical files for staging.

## Runtime proof

The final diagnostic against `http://localhost:3003` recorded 15/15 terrain tile responses as HTTP 200, all `image/png`, across zoom levels 5, 9, 10, and 11. It recorded no failed responses, decode errors, or MapLibre terrain errors. The only browser noise was the expected local WebSocket 403 response.

The final Teesside capture was taken three seconds after enabling terrain with RF Coverage disabled. It shows continuous relief across multiple adjacent terrain tiles without the former isolated hard-edged square.

Public gates:

- `https://ukmesh.com`: HTTP 200, `text/html`
- `https://test.ukmesh.com`: HTTP 200, `text/html`
- `https://ukmesh.com/terrain-tiles/5/15/9.png`: HTTP 200, `image/png`
- `https://test.ukmesh.com/terrain-tiles/5/15/9.png`: HTTP 200, `image/png`

## Evidence

- Decisive deployed Teesside screenshot: `/home/ben/ukmesh/meshcore-analytics/terrain-v6-evidence/terrain-v6-teesside-final.png`
- Final standard diagnostic screenshot: `/home/ben/ukmesh/meshcore-analytics/terrain-v6-evidence/terrain-diag2.png`
- Successful H4 screenshot: `/home/ben/ukmesh/meshcore-analytics/terrain-v6-evidence/h4-ts256-minabs-terr-e3-after.png`
- H4 measurements: `/home/ben/ukmesh/meshcore-analytics/terrain-v6-evidence/h4-ts256-minabs-terr-e3.json`
- H3 center/pan/zoom captures: `/home/ben/ukmesh/meshcore-analytics/terrain-v6-evidence/terrain-h3-center.png`, `terrain-h3-pan.png`, and `terrain-h3-zoom.png`

All commits are local only; nothing was pushed.
