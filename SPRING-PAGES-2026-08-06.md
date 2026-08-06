# UKMesh Spring Pages — 2026-08-06

Scope: items 10, 12, and 14 on `/home/ben/ukmesh/meshcore-analytics`, branch
`main`. The separate `~/ukmesh/meshcore-health-check` application was not
modified. No GitHub push was performed.

## Decisions

### Item 10 — observation register: resolved by absence

There is no dedicated observation-register page. The `PUBLIC_ROUTES` manifest
contains no `/register` or `/observations` route, and the site navigation has no
observation/register entry. A case-insensitive `rg` search across `frontend/src`
found only ordinary observation data labels and models in topology, stats,
packet detail, feed paths, and map statistics, plus the observer-station
registration form embedded in `UKCompanionPage`. Stats, repeater search, and
owner portal have no hidden register page. No page was invented or changed.

### Item 12 — Spam Watch: kept

The live host endpoint returned HTTP 200 and a valid page-shaped payload:

```json
{"ongoing":false,"activeIncidents":0,"totalIncidents":0,"messagesLast24h":0,"observersInvolved":0,"lastIncidentAt":null,"updatedAt":"2026-08-06T12:49:27.141Z"}
```

The page’s incidents request, `/api/spam/messages/incidents?limit=200`, also
returned HTTP 200 with `minConfidence: 0.5`, `limit: 200`, `returned: 0`, and an
empty `incidents` array. A Googlebot Playwright check of the real public URL
observed HTTP 200 JSON for both requests, rendered `Spam Watch` and
`No ongoing spam detected`, and rendered no alert/error element. The page is a
useful transparency surface in this valid clean state, so no Spam code change
was necessary.

### Item 14 — Health page: removed

Removed the page module, lazy import, route component/type, `/health` content
route, obsolete `/status -> /health` redirect, `showHealth` prop, Health nav
entry, unused UK layout flag, page-specific status CSS, and stale E2E page
fixtures/assertions. Direct page-link searches for `/health` in `frontend`
return no results. A literal substring search leaves only the unrelated
`/api/observers/health` test endpoint and the external
`https://healthcheck.ukmesh.com` link; neither links to the removed page. The
external Health Check link remains intentionally because it belongs to the
separate application.

## Quality and deployment

- `cd frontend && npx tsc --noEmit` — passed.
- `cd frontend && npm test` — 77/77 passed.
- `cd frontend && npm run lint:css` — passed.
- `cd frontend && npm run build` — passed; only existing large-chunk warnings.
- Built both requested tags: `meshcore-analytics-app:spring-pages` and
  `meshcore-analytics-website:spring-pages`.
- The new not-found marker was present in both shipped asset sets, so both
  `app-ukmesh` and `website-ukmesh` bundles changed and both were deployed.
  No backend image/service was changed.
- Final deployed image IDs:
  - app: `sha256:5f278c9796c67402c644961555feb8a52c4ee6eb4cc1f41c94b9b79276c80b55`
  - website: `sha256:56885f493cca824e2e7cb7ce6cda7b08748747762b00ffa84bdfb9432db9e67b`
- `.env` was updated only for `APP_IMAGE` and `WEBSITE_IMAGE`; `BACKEND_IMAGE`
  and secret/configuration lines were left unchanged.

## Live verification

- `GET http://127.0.0.1:3000/api/health` — HTTP 200, `status: healthy`.
- `GET http://127.0.0.1:3000/hopreach/api/nodes` — HTTP 200.
- Googlebot Playwright against `https://ukmesh.com/health` — the SPA transport
  returned 200, but the rendered page was the normal `That page is not on the
  mesh` not-found state and the exact `Health` nav link count was zero.
- Googlebot Playwright against `https://ukmesh.com/status` — same normal
  not-found state; no legacy redirect to the removed Health page.
- Googlebot Playwright against `https://ukmesh.com/spam` — document HTTP 200;
  status and incidents API responses HTTP 200 with JSON content type; heading
  and clean state visible; alert count zero.

The browser emitted the same pre-existing unauthenticated owner-session and
service-worker MIME console noise seen in the pre-deploy baseline; neither
affected page data loading or rendered state. Evidence image:
[SPRING-PAGES-2026-08-06.png](./SPRING-PAGES-2026-08-06.png).

## Local commit

Implementation and evidence were committed locally in `1c3841c`
(`chore(website): spring-clean public pages`). No remote push was attempted.
