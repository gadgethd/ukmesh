# UK Mesh feed history — 2026-08-06

Implemented per-channel historical messages without changing the WebSocket snapshot path.

- Added `GET /api/feed/messages?channel=<name>&limit<=50`, scoped by the existing public network/observer filters.
- Channel history searches a bounded 90-day window, matches decrypted summaries (`[bot] ...`, case-insensitive) and configured channel hashes, deduplicates packet observations, applies public privacy filtering, and returns at most 50 messages.
- `UKFeedPage` loads history on channel selection, merges it with retained live packets, preserves the existing `all` view, and shows full date/time values for historical rows.
- No node-identity files or migrations were changed.

## Deployment

Local commit: `f5a2a2b` (`feat(feed): load historical channel messages`)

The requested tagged images were built and pinned in `.env`:

- backend: `meshcore-analytics-backend:feed-hist` — `sha256:f690a18391d2ad71148469f7ff34043e1cca07b6747478e04fa73d9c35b6f9dc`
- app: `meshcore-analytics-app:feed-hist` — `sha256:54544b041cd11f68fae58b748ab1837ab05d31d4251cad4e2887218542837994`
- website: `meshcore-analytics-website:feed-hist` — `sha256:77f1a456e00e1444b42e48d0d2f7931cc0e3a4a914d2446a6ea9c8a0fdc93952`

Only `backend`, `app-ukmesh`, and `website-ukmesh` were recreated with:
`docker compose -f docker-compose.yml -f docker-compose.live.yml up -d --no-deps --force-recreate ...`

## Verification

- `GET /api/feed/messages?channel=bot&limit=50&network=ukmesh`: HTTP 200, 50 unique messages, all `[bot]`-prefixed, newest `2026-08-05T07:29:51.254Z`, oldest `2026-07-27T16:45:51.547Z`.
- `/api/health`: healthy.
- HopReach internal HTTP check: 200.
- Allowed-origin WebSocket check: opened and received `initial_state` with 200 messages.
- Served app asset `assets/UKFeedPage-DU39D7Xg.js` and website asset `assets/UKFeedPage-CmhTQ_aR.js` both contain the history endpoint and status text; the backend image OpenAPI bundle contains `/api/feed/messages`.
- Backend: 271 tests passed. Frontend: 76 tests passed; builds, OpenAPI, CSS, and performance-budget checks passed.

Screenshot: [quiet bot channel history](./FEED-HISTORY-2026-08-06-bot.png)
