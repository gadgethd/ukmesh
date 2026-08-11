# UKMesh spring dashboard — 2026-08-06

Items 4, 17, and 18 are implemented and deployed locally on `main`.

## What changed

- Owner dashboard identity data now comes from the canonical
  `node_identity_nodes` view and exposes `canonicalId` and all merged/source
  `members`. The owner display groups same-name authorized identities as one
  entry while retaining an authorized source key for existing live queries.
  Member keys and the canonical ID are visible on each entry.
- The owner dashboard no longer has Dashboard/Live/Settings navigation. All
  existing identity, telemetry, map, alert, trend, link, sender, heard-by,
  and packet sections are stacked on one page.
- The settings UI, its inactive controls, roadmap/"coming soon" payload, and
  related dead CSS were removed. The backend alert API and worker remain as an
  active operational delivery path.

## Verification

Live owner grouping from the deployed backend produced exactly two relevant
entries: one `GNOME-MSG-RPT` entry with 3 member keys and one `2E0MTU RPT
Hilperton` entry with 2 member keys. The screenshot
[SPRING-DASHBOARD-2026-08-06.png](./SPRING-DASHBOARD-2026-08-06.png) records the
browser smoke result.

The Googlebot Playwright smoke loaded the real public owner route
`https://ukmesh.com/login`. This is the owner route in the deployed build;
`https://app.ukmesh.com` is the map-only app build. Because no owner MQTT
credential is available on this host, the smoke used the read-only live
grouping payload from production and intercepted only the authenticated session
and live requests; the deployed public JS/CSS and route were loaded from the
real site. The temporary session response uses the neutral label `owner`, not
a proof label. Assertions found 2 identity cards, 1 GNOME card, 1 2E0MTU card,
member counts 3/2, zero section tabs, zero settings UI, and zero
`spring-dashboard-proof` text. No proof-mode toggle or proof token is present
in source or in the shipped app image.

Quality gates:

- Backend `npx tsc --noEmit`: pass.
- Backend `npm test`: 273/273 pass (271 existing tests plus 2 grouping tests;
  no pre-existing failures).
- Frontend `npx tsc --noEmit`: pass.
- Frontend `npm test`: 76/76 pass.
- Frontend `npm run lint:css`: pass.
- Frontend `npm run build`: pass; only existing large-chunk warnings.
- `GET http://127.0.0.1:3000/api/health`: `healthy`.
- `GET http://127.0.0.1:3000/hopreach/api/nodes`: HTTP `200`.

## Deployment

Deployed services use the spring-dash image tags and are healthy:

- Backend: `sha256:6a11941684f87f029bbcf98b50ec2764f92ede5ea6f5f8d9c029ba7a0cac1164`
- App: `sha256:30b286f26b8fdf1b8a04a41585475822bd80658a9057c627c4df626370fc12b2`
- Website: `sha256:96ab70c6b4f168d6dc11c036bc7119a578b6b22d89ae8e505544d68cfbe87b92`

Only `APP_IMAGE` and `WEBSITE_IMAGE` were updated in the ignored `.env`, as
required. The backend image was supplied explicitly for this rollout without
altering other `.env` lines. The implementation commit is `1e5bc3b`; this
screenshot/proof-audit update is recorded in the follow-up local commit.
