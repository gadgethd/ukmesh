# Changelog

## 2.0.0 — candidate, unreleased

This records the prepared candidate. Publication and production cutover require
completion of the candidate gates and separate release authorization.

### Website and dashboard

- Public pages load feature code and styles on demand. Stats load their initial
  sections separately and open the decoded-path map only when requested.
- Regions and topology use lightweight embedded maps. Basemap recovery and
  fallback behavior cover failed styles, missing sources and worker errors.
- Feed rows support measured heights, wrapping, prepends and viewport changes.
- Service-worker v10 retains v9 for seven days, including offline old-tab lazy
  chunks, explicit update activation and retained-cache rollback.
- Dashboard map updates avoid rebuilding unchanged node geometry. Coverage
  polling observes document/layer visibility and pending-work deadlines.
- Optional field performance reports contain bounded route and Web Vitals data,
  without node/owner identifiers, cookies, referrers or event attribution.

### Backend and operations

- Public JSON uses scope-aware validators, mandatory revalidation and fresh
  privacy-generation fences. Runtime configuration and owner data remain
  `no-store`. Stats section selection is opt-in; default response DTOs remain.
- Immediate packet-batch collection reduces persistence delay while preserving
  transaction, durable duplicate, privacy and acknowledgement boundaries.
- Broker correlations, lease observations and spam-detector state have explicit
  bounds and occupancy/health metrics. Incomplete spam evidence refreshes retain
  the previous complete snapshot and report degraded coverage.
- Region snapshots recover outside HTTP through a bounded supervised worker.
  Node responses and cached links validate current privacy before delivery,
  including conditional responses.
- Timeline identity and packet-hash grouping reduce lookup and sorting work. Timeline, RF,
  observer-health, link-history and firmware responses fence privacy at delivery;
  busy relay rows no longer block best-effort live-map reactivation.
- Timeline snapshots have bounded retention and coalesced refreshes. Content is
  fresh for 30 seconds and expires after 90 seconds; cached delivery still checks
  current privacy authority, including conditional responses.
- Link-worker geometry dependencies load when needed, reducing measured startup
  time and memory. RF calculations, queue protocols and authority remain intact.
- A local contributor stack provides invented-data and private archive profiles,
  migrations, readiness checks and repeatable ingest/API/SQL measurements.
- Backup restore drills support Timescale restore hooks in both databases,
  isolate outbound traffic, capture policy state and sign candidate evidence.

### Breaking changes and upgrade requirements

- Node contributor tooling and runtime images use Node 26. Follow `.nvmrc`.
- The schema separates `core`, `mesh`, `analytics` and `ops`. Run explicit role
  bootstrap and migrations before application startup. Legacy compatibility
  views do not guarantee old-image write compatibility.
- Migration 136 certifies region/spam derived data against its privacy
  generation. Existing uncertified data is withheld until rebuilt.
- Owner authorization uses its explicit database/schema and server-side session
  expiry/revocation. Preserve and verify grants; image rollback does not reverse
  schema, data, cookie or session changes.
- Historical identity/path backfills remain explicit bounded operator jobs.
  Application startup does not run them or enable lifecycle policies.
- Existing legacy coverage `410 Gone` contracts and dormant rollback assets
  remain through the seven-day rollback window.

Read the [2.0 upgrade guide](docs/upgrade-2.0.md) before selecting a migration
path. The actual prior deployment inventory and isolated upgrade/old-image
rollback evidence are required. A synthetic disaster-restore drill alone does
not establish production rollback compatibility or recovery objectives.

### Candidate limits

Production publication, deployment and CDN cutover remain pending. Final
candidate security, sustained load, browser quality and clean-clone onboarding
must be bound to the reviewed candidate SHA. Performance measurements are
fixture-specific and do not establish production capacity. Detailed reproduction
and remaining operational gates are in the [performance guide](docs/backend-performance.md),
[upgrade guide](docs/upgrade-2.0.md) and [release runbook](docs/runbook-release-rollback.md).
