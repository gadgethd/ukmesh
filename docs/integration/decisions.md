# Integrated service decisions

## Feed ownership

- The Beacon directory and conversation already own cursor paging, packet deduplication, region changes and scroll anchoring. Build a small Beacon entry around that view. The full Beacon app also creates packet and analytics routes and subscribes to events those routes need. The public entry uses one channel subscription and its own query cache.
- The UKMesh Packet Feed owns packet, watch and path behaviour. Keep it in the UKMesh website at `/feed/packets`. Keep the old feed active while the Beacon read and live contracts are being verified.
- Beacon public routes cannot inherit the broad LAN proxy. A known decryption key or one-byte channel hash cannot authorize publication. The public gateway must validate the exact channel and observation against UKMesh visibility, and it must treat enrichment and old cached data as subject to revocation.
- The restricted history source lives in the UKMesh qualification scripts. Beacon owns frame decoding and reconciles its short hash with the full UKMesh packet identity. The reader binds its cursor to the network set and privacy generation and checks the authority again after reading. Two hearings with equal times and content remain distinct by observation UUID. Tests use an owned database and a disposable read-only role. A public channel policy, authenticated message projection, and live delivery are required before routing user traffic.
- The private Beacon authority shares the UKMesh channel registry and ready visibility check. Channel names and the one-byte wire hash are unsuitable keys for publication. Export only approved material, identify it by its full key fingerprint, and carry a registry revision alongside the visibility generation. The Beacon credential stays separate from the CoreScope stream credential.

## Map ownership

- CoreScope currently uses host-root URLs while UKMesh owns `/api`, `/ws`, terrain, RF coverage and the installed app’s service worker. Move the fork’s public client URLs behind one deployment config and give the gateway explicit routes. Keep authority and acquisition separate. The public server must use the approved projection, and the browser must expire the previous generation when the authority changes.
- The isolated preview edge separates static files, CoreScope’s narrow read routes, and the UKMesh runtime configuration. It checks the projection before forwarding CoreScope data or accepting a WebSocket upgrade. Cookies and authorization headers stay within their original services. The preview sends RF reads through the candidate projection. That projection matches network tiles to the current generation and rechecks each archived repeater's visibility and coordinates. The old RF mount stays with the legacy app during qualification.
- The cutover previews share the credential-stripping proxy settings. CoreScope requests the existing UKMesh region and path APIs through an explicit allowlist. The UKMesh service worker keeps its existing scope and retains the previous cache generation. It can cache versioned CoreScope scripts and styles. API, RF, and runtime authority responses stay on the network.
- The UKMesh activity timeline owns packet attribution and canonical region membership. Cache and echo the exact region on scoped reads. CoreScope refreshes the rolling snapshot when playback opens and removes it when the region or current visibility authority changes. Highlights use the current visible node positions.

## Health Check ownership

- The deployed pin, fork main and upstream had diverged. The merge kept the deployed origin and session checks, and the refactor gave WebSocket transport its own module. The app and share screens use the same guided layout. A restored version-1 store verified historical targets and receipts in the built image.
- The Health Check gitlink is the source pin for DEV, CI and release builds. BuildKit fetches that exact commit from the fork before installing dependencies from the fork’s lockfile. This binds the image source to its revision label even when a local checkout contains additional assets. DEV and release builds verify the selected pin. Remove the external lockfile and the image overlays together.

## DEV qualification

- Beacon readiness needs native connection events, process identity and the current broker epoch. Streaming hours of access logs through Docker repeatedly exhausted the probe budget. Filter the same journal by container and event fields when the service uses journald. The startup deadline and the separate intake, broker and target identity checks still apply.
- A completed companion build can disappear between Docker listing and inspection, aborting a valid transition. Retry only after confirming a listed ID disappeared. Reinspect the entire current set and require exact full IDs before comparing owned service and broker identities.

## Browser journeys

- The repeater input owns immediate edits, including composition. Persist the query in the URL for reload and history navigation. A router transition can arrive after Chromium has started composing the next edit.
- Give each mobile feed status metric an explicit grid slot. Adding a freshness label shifted the other labels through the old numbered slots and let changing text move the packet list.
- Share the Find interaction across the dashboard and Regions browser suites. Both suites need the visible mobile search, and Layers has no search input. Check the website navigation's visible tab order and retain the privacy, alias and focus restoration assertions.
- Browser tests wait for the owner or topology fixture requests before checking the dependent UI. Owner timeout tests inspect the loading panels together while the request is pending. Owner comparison and save tests wait for the relevant responses before checking the results. This keeps cold startup and CSRF lookup time outside those assertions.

## Stats refresh

- Give multibyte fact coverage checks and rebuilds their own budget. They share the analytics pool and statement limit with chart reads, and reuse the chart cutoff and cancellation. The chart budget was also limiting the eight-day rebuild when privacy authority changed.
- Count identical GroupText summaries before classifying channels. DEV kept accepting MQTT while repeated channel counts exceeded the chart query budget. The scan still applies the network and observer predicates and the database privacy policy. Grouping the counts keeps the complete 24-hour total and the existing channel labels.
- Let the Packets view load observer codes from a scoped feed lookup. Its region picker was requesting a complete chart calculation for one field. The lookup keeps the same seven-day observation window and checks the current visibility generation before delivery.
- Remove the historical Stats page and its periodic chart refresh together. Keep the lightweight summary warmup and serve existing chart API clients on request. The historical widgets, route budgets and browser checks belong to the retired page.

## Runtime isolation

- Static origins verify their own assets and resolve upstreams per request. The public tunnel uses the static origin gate, and monitors can report backend failures even if the backend never becomes ready. Keep aggregate readiness for release gates and diagnostics.
- Owner ACL reconciliation owns its startup retry. Keep protected owner access unavailable until the first reconciliation completes. Public HTTP and packet intake can start independently. The operator diagnostics and deployment readiness still check the applied ACL and reviewable inventory.
- A stable mounted baseline filename lets an operator activate validated inventory evidence by atomic replacement. Keep versioned exports and prior bytes for rollback. Accepted MQTT credentials, approved owner grants and switchable service connection settings have different lifecycles.
- Rotate the ingest password in the transport runtime. Validate via a separate clean client, keep the durable session and acknowledge only after the existing persistence boundary. The mounted password belongs to a single broker and principal, so host and identity changes use a separate handoff.
- Let confirmed owner provisioning write audited database grants. The running reconciler applies those rows alongside legacy environment grants. Use readback from that reconciler and the broker rather than restarting the backend to make the environment authoritative again.
- Link backfill waits for its migration and queue consumer. HopReach starts when its HTTP upstream is running. Neither needs the owner's aggregate deployment readiness to be green.
- Give the ingest process the existing persistent MQTT session, commit boundary and forwarding outbox. The old process coupled intake with HTTP startup, WebSocket publisher initialization, owner ACL work and API resolver state. Publish live events through Redis independently of WebSocket startup, keep the current privacy checks on both sides and keep path cache work with the API. Treat these live notices as best-effort; PostgreSQL remains the recovery source for WebSocket snapshots.
- Give operators separate API and intake health checks. Aggregate deployment readiness still verifies the database, MQTT delivery and owner ACL. Rolling out the split must hand off the existing client ID without running two consumers on it.
- Apply restart policies to the full DEV dependency chain. Persist sample Redis queues on a named volume and let only the optional archive player pause at the disk reserve. A service or host restart should preserve work already admitted; one-shot jobs and deliberately stopped services keep their own lifecycle.
- Let readers refresh WebSocket snapshots after startup. Idle refreshes of the 24-hour message history were spilling to disk even with no readers. Use the analytics pool for that history and bound concurrent snapshot builds to keep HTTP admission separate.

## Sync with main

- Merge main through `7bb40a7` into the DEV line based on `f8d0a96`. DEV owns the split MQTT/API runtime, local replay and resource protections, the companion integrations, and the retirement of the historical Stats page. Bring across compatible fixes through their current owners.
- Port packet identity, radio status, owner grants, and observer notification metrics into the repositories and workload modules that own those features. Main still refers to the retired database and metrics facades. Its new migration numbers collide with changes already applied on DEV.
- Keep on-demand chart compatibility separate from recurring background work. The existing stats summary and feed-region lookup do not need to restore the historical page, its refresh loop, or an alert that assumes periodic chart runs.
- Keep DEV integrations on the local broker and test endpoints. Shared image, delivery, and dependency changes must preserve separate API and ingest readiness. Host-specific production settings stay outside DEV.
- Run all 15 compressed-data cases in one dedicated TimescaleDB job on the repository's pinned Node version. The separate job runs independently of the other backend checks. Keep the full frontend dependency audit.
- Use Vite 8.3.6 with Rolldown 1.2.16 for DEV builds. Vite 8.3.8 brings in a Rolldown binary that raises SIGILL on the 4-core DEV VM. Recheck native compiler imports on DEV before moving those pins.
- Keep the existing per-request chart budget defaults on DEV. Operators can override them, and the multibyte backfill retains its separate budget. The longer production defaults were set for the retired scheduled chart job.
- The owner pool explicitly searches the two supported schemas. Consolidated installs use `core.owner_sessions` and the standalone DEV database retains its existing `public.owner_sessions`.
- Start Prometheus, Alertmanager and the synthetic monitor independently of the alert receiver. Alertmanager and the synthetic monitor already retry deliveries. DEV still requires its one-shot receiver bootstrap before sending authenticated alerts.
- Treat the MeshOS ending as an unverified format annotation and keep the summary text intact throughout the feed. The one-byte channel hash can collide, so inspection prefers the channel already identified by the decoder.

## DEV performance audit

- Freeze the merged source at `9dac76f` for matched trials. A prior archive publisher receipt covered 480,000 broker acknowledgements, with only 190,642 of the expected hashes present in the corresponding UKMesh store. The fixture controller owns the publication lease and application persistence evidence. Keep failed receipts and verify the disk reserve before staging any additional fixture data.
- Cache the compiled public prefix index inside its reader. The previous query aggregated prefixes at every authority check and the process rebuilt the index each time. A single SQL statement still verifies certification and rematerialisation at each fence and fetches prefixes when the generation changes.
- Let one channel decoder own GroupText authentication and attribution. The ingestion and backfill paths decoded the whole packet again for each key candidate to recover a label. Cache validated payload decode by canonical identity within the current registry and keep label and structural marker tied to the authenticated key.
- Keep the current short-search plan. On a synthetic 6,150-node Timescale fixture, the set-based candidate plan returned identical rows and reduced rare and missing search time, but the common one-character case rose from a median of 9.91 ms to 95.15 ms. Retained 1× and 10× results will be required for any replacement.
- Resolve a channel detail page's sibling decryption once per scoped UID. Candidate selection still applies the complete 90-day search and the representative, exact-hash fallback and final public privacy rules stay with the repository.
- Capture observation and visibility versions with resolver input, and let the resolver own shared work and publication. Retire workers before replacing them. WebSocket snapshot builders share work by scope and generation behind existing socket admission, and account for each retained representation.
- Move repeated packet visibility rules into PostgreSQL. Rematerialisation preserves its claims and certifies only after the same framing and privacy checks used on new packets. Link jobs share physical pairs only for the exact committed generation revision. Keep terrain evaluation inside the existing transaction pending lock attribution.
- Reuse the RF manifest index within its verified file revision and check authority and tile bytes on each response. UI state owners publish changes to geometry, messages, session updates and packet order so renderers avoid repeating unrelated work.
- Component comparisons on this VM justified keeping the GroupText decoder, shared WebSocket encoding and RF manifest index. Three paired 4,000-frame GroupText runs produced identical summaries with median batch time 7,317 to 853 ms. Three 32-client reconnect encoding runs retained the same frames and took 4.4 to 6.2 times less wall time. The RF 5,000-tile fixture returned identical PNG bytes and reduced median loopback p95 from 189.22 to 20.68 ms with mocked authority. These measurements isolate the changed work.
- Keep both CoreScope store modes and the current graph transaction until matched profiling can verify those conditional changes. The retained fixture and sustained acceptance runs need capacity above the 10 GiB reserve. Keep the existing gates and the running stores during component qualification.
