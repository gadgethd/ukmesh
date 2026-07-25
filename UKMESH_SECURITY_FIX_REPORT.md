# UKMesh security remediation report

## Outcome

All 24 findings in `UKMESH_SECURITY_REMEDIATION.md` have been implemented in the working tree based on commit `363380fca85c4dd9acf5b14fde745bdc04a458db`. A Codex Security review of the complete repair diff found 16 additional bypass candidates during implementation; each was fixed and independently revalidated against the final source. No candidate remains reportable in the final reviewed snapshot.

This report is an implementation and hand-off record. It does not claim that deployment-level integration was exercised: no live PostgreSQL, Redis, Mosquitto, Cloudflare, Nginx, or multi-replica environment was available in this workspace.

## Audit findings, ranked by original severity

| Severity | ID | Finding | Resolution implemented |
|---|---|---|---|
| High | UKM-002 | Client-selected MQTT client IDs grant node ownership | Ownership authority now comes only from active, verified, unrevoked `owner_account_nodes` grants populated by trusted operator configuration/database paths. MQTT client IDs and broker observations are non-authoritative. Owner cookies contain identity and expiry only; every protected request resolves current grants server-side. ACL reconciliation consumes only the verified grant snapshot and writes exact per-node topic rules. |
| High | UKM-003 | Denied MQTT publishes are converted into ownership/ACL entries | Denied-publish and connection-log processing is audit-only. `connectionMonitor` and the disabled-by-default ACL watcher cannot create grants, edit ACLs, or reload Mosquitto. `ownerAccess` is the sole reconciliation path and reads verified grants only. |
| Medium | UKM-001 | Stored XSS through attacker-controlled node/packet metadata | Map popups now build fixed DOM nodes and assign untrusted values with `textContent`/text nodes. No attacker field is passed to `setHTML`. Nginx adds a restrictive Content Security Policy and related browser hardening headers. |
| Medium | UKM-004 | Owner authorization survives revocation in cookies/caches | Authorization snapshots were removed from sessions. Cookies hold only authenticated identity and expiry; dashboards and owner actions query current grants. Cached dashboards cannot bypass revocation because authorization is checked before cache reuse. |
| Medium | UKM-006 | Forwarded headers allow client-IP spoofing | Express and WebSockets share an exact trusted-peer set. A forwarded address is accepted only from a configured or DNS-resolved proxy peer and only when it is a valid IP. Nginx overwrites, rather than appends, forwarding headers. Backend and frontend origin ports are bound to `127.0.0.1` in Compose. |
| Medium | UKM-007 | Test traffic contaminates production-derived models | The public production scope is the unified UKMesh legacy-compatible set and explicitly excludes `test` rows and `meshcore-test` topics. Spam, path learning, path resolution, stats, node/link reads, and recompute tooling now use the same scope rules. Current test nodes and stale test sightings cannot enter production path models. |
| Medium | UKM-009 | Public APIs and statistics mix test/production data | Public `network=all` requests fail closed. Request-scope and SQL helpers preserve explicit test isolation while mapping legacy production labels to UKMesh. Observer-scoped queries retain the same network/topic separation. |
| Medium | UKM-010 | Private-node data leaks through public outputs | Private nodes are omitted from public node lists, exports, topology, coverage, WebSocket initial/live state, activity timelines, decoded paths, lazy paths, inferred nodes, and learned path priors. Packet queries exclude private endpoints and intermediate relay prefixes. Privacy checks cover valid one-, two-, and three-byte hashes and reject malformed path metadata, NULL elements, non-hex elements, and inconsistent lengths. |
| Medium | UKM-011 | Reversible coordinate masking exposes exact locations | Private-node projectors no longer derive a deterministic public offset from exact coordinates. Private coordinates and location-linked fields are returned as `null` or the complete private row is omitted, depending on the public collection. |
| Medium | UKM-014 | Unbounded process caches permit memory exhaustion | Shared caches use `BoundedTtlMap` with entry, weight, and TTL limits plus physical expiry sweeps and metrics. Observer-keyed work is not persisted in shared caches. In-flight work maps have hard concurrency ceilings and are cleaned on both resolve and reject. Identity-bearing chart responses are recomputed so a privacy opt-out cannot remain in a stale cache. |
| Medium | UKM-016 | Path-resolution work and caches are unbounded | Path resolvers use bounded candidate inventories, prefix buckets, caches, worker pools, queue lengths, input sizes, and execution deadlines. Public DTOs are explicit allowlists and cannot serialize internal resolver state. |
| Medium | UKM-017 | Link-job queue is unbounded and loses/coalesces work unsafely | New producers use the Redis `meshcore:link_jobs:v3` keyed queue. Atomic Lua admission enforces count, per-payload, total-byte, pending-delta, retry, and dead-letter caps while coalescing the same logical job. Claims use 128-bit random tokens and leases; recovery checks current state and expiry atomically in Lua, so it cannot requeue another worker's live claim. Arrivals during processing remain as a bounded pending delta and are requeued exactly once. |
| Medium | UKM-018 | Spam analysis has quadratic/unbounded cost | Repository reads deduplicate observations by `packet_hash` before applying the newest-logical-message budget. Message, observer, cluster, sender-comparison, incident, character, and runtime budgets are bounded. The analyzer aborts on deadline and persistence uses a scoped advisory lock so overlapping runs cannot corrupt state. |
| Medium | UKM-020 | Health checks synchronously depend on expensive database work | Liveness no longer runs heavy database aggregation. Readiness uses a bounded cached snapshot with explicit stale/error behavior, keeping health probes non-blocking during database degradation. |
| Medium | UKM-021 | Prefix collision/path-learning retries can grow indefinitely | Prefix buckets overflow closed at 129 candidates. Eligible nodes, links, packets, output rows, motif choices, and derived-loop time are capped. Sentinel rows abort before maps are materialized or model tables are replaced. |
| Medium | UKM-022 | WebSocket connections and per-client work are uncapped | Handshake attempts, total connections, per-IP connections, initial-state work, cache size, per-client buffered output, and heartbeats are bounded. Client identity uses the exact trusted-proxy helper. Privacy is checked before Redis publication and again before public fan-out; an unready privacy index fails closed. |
| Medium | UKM-024 | Public path responses expose internal hash-anchor/resolver state | Public beta and multi-observer path results are deep allowlist projections. Internal sticky mappings, region links, debug fields, inherited fields, getters, and unknown nested values are rejected or omitted. |
| Low | UKM-012 | Status metadata can overwrite another node's state | Status identity is accepted only when the decoded source is present and the decoded status origin is missing or exactly matches. Malformed/mismatched identities are rejected before authoritative node updates. |
| Low | UKM-013 | Rawless adverts can spoof node identity | Only successfully decoded TX adverts may establish advert source identity. Rawless JSON envelopes and envelope `origin_id` fields are retained, at most, as non-authoritative observational payload data. |
| Low | UKM-015 | Planned-coverage queue and result lifecycle are unbounded/shared | Planned jobs require a live worker heartbeat and use bounded outstanding/queue state with TTL-backed metadata and database expiry. Completed/failed fingerprints are never reused. Each request receives a collision-checked public handle; shared internal IDs never leave the API, and deleting one handle cannot cancel or delete another caller's computation. |
| Low | UKM-025 | Packet/path history reads are unbounded | History endpoints use bounded windows, limits, pagination/byte budgets where applicable, exact network/observer scoping, and the shared privacy predicate before row mapping. |
| Low | UKM-026 | Lazy path resolution amplifies database work | Canonical observations, hop rows, candidate sets, prefix widths, and result construction are capped. Queries reject private/malformed path rows before resolution, and the public result contains only bounded allowlisted data. |
| Low | UKM-027 | Path-learning construction has quadratic/unbounded behavior | Node, link, packet, adjacency, motif, and output work has hard caps and recurring deadline checks. Read-time eligibility joins also prevent stale private/test IDs from leaking if an hourly rebuild fails. |
| Low | UKM-029 | CSV exports permit spreadsheet-formula execution | All exported cells pass through one CSV encoder. Values beginning with spreadsheet formula sigils (`=`, `+`, `-`, or `@`, including leading whitespace cases) are neutralized before RFC-style quoting/escaping. |

## Additional bypasses found and closed during repair validation

| ID | Pre-fix risk | Final control |
|---|---|---|
| DISC-NODES-001 | Observer-scoped known-node inventory could infer a private intermediate relay | Separate network-wide identity-only suppression inventory; observer scope remains only for visible anchors. |
| planned-coverage-shared-handle | Same-coordinate callers shared a deletable handle/internal ID | Unique collision-checked public handles; handle-only deletion; internal ID replaced in ready DTOs. |
| PRIVATE-NODE-STABLE-IDENTITY-LEAK | Stable private IDs/activity survived in nodes, WebSockets, exports, or timeline | Complete omission plus timeline endpoint/relay filtering before aggregation; no stable fallback DTO. |
| mqtt-acl-case-collision-revocation | Case-folded username matching could revoke the wrong ACL block | Literal case-sensitive principal matching with a regression test. |
| mqtt-router-envelope-source-spoof | Envelope `origin_id` fallback could become router source identity | Decoded-byte identity only; no envelope fallback. |
| DISC-DB-PRIVACY-002 | Packet SQL filtered private endpoints but not relay prefixes | Shared endpoint/path privacy clause across recent, message, event, detail, history, and advert queries. |
| DISC-INDEX-PROXY-001 | Container proxy identity collapsed limits or trusted spoofed headers | Exact peer trust, single validated forwarded IP, overwritten Nginx headers, loopback origin ports. |
| OWNER-ACL-RELOAD-NOT-RETRIED | A failed Mosquitto reload left stale effective ACLs indefinitely | Pending reload state starts true and remains true until a later reconciliation reload succeeds. |
| DISC-LAZY-PRIVATE-PREFIX-001 | Lazy resolution exposed private or malformed raw prefixes | Both observation queries fail closed on private endpoints/prefixes and malformed metadata before DTO creation. |
| path-learning-scope-filter-bypass | Stale test/private endpoints entered or remained in public learned models | Rebuild filtering plus read-time eligibility joins for every raw-ID prior table. |
| path-learning-posthoc-budget | Link/output caps ran after unbounded materialization | SQL sentinel limits and deadline checks precede maps and transactional replacement. |
| planned-coverage-reuse-oracle | Completed-result reuse revealed prior coordinate queries | Only active queued/leased jobs coalesce; completed/failed fingerprints are removed and all callers get fresh public handles. |
| link-recovery-live-claim-race | Startup recovery could requeue a live claim and double-count topology evidence | Atomic lease/state recovery in Lua and 128-bit claim tokens. |
| SPAM-CANDIDATE-ROW-STARVATION | Duplicate observations consumed the raw-row budget before deduplication | `DISTINCT ON (packet_hash)` precedes recency ordering and limiting. |
| PRIVATE-DECODED-PATH-IDENTITY-LEAK | Decoded stats exposed or cached a private relay prefix/position | Both windows exclude private prefixes; all fields suppress atomically; identity-bearing chart responses are not reused across requests. |
| DISC-WS-PRIVATE-ONEBYTE-001 | One-byte and malformed relay paths bypassed privacy checks | Two-hex private prefixes plus fail-closed runtime/SQL validation for NULL, sparse, non-string, non-hex, wrong-length, and invalid-size paths. |

## Key implementation areas

- Owner authorization and ACLs: `backend/src/db/ownerAuth.ts`, `backend/src/owner/ownerService.ts`, `backend/src/owner/ownerSession.ts`, `backend/src/owner/ownerAccess.ts`, `backend/src/mqtt/aclManager.ts`, `backend/src/mqtt/connectionMonitor.ts`.
- Network/privacy boundaries: `backend/src/http/requestScope.ts`, `backend/src/api/utils/networkFilters.ts`, `backend/src/api/utils/privateNode.ts`, `backend/src/db/index.ts`, public routes, `backend/src/ws/server.ts`.
- Proxy and deployment policy: `backend/src/http/trustedProxy.ts`, `backend/src/index.ts`, `nginx.app.conf`, `nginx.website.conf`, `docker-compose.yml`, `.env.example`.
- Resource controls: `backend/src/cache/boundedTtlMap.ts`, path resolver/pool/cache modules, spam modules, `backend/src/queue/publisher.ts`, `viewshed-worker/worker.py`.
- Output safety: owner/map popup code, `backend/src/pathing/pathingPublicDto.ts`, decoded-path projection, and `backend/src/api/utils/csv.ts`.
- Database rollout: migrations `010_spam_scope_isolation.sql`, `011_packet_hash_network_time_index.sql`, and `012_planned_coverage_lifecycle.sql`.

## Verification completed

The final hand-off must retain these gates:

```text
cd backend && npm run typecheck
cd backend && npm test
cd frontend && npm run build
python -m py_compile viewshed-worker/worker.py
python -c "import yaml; yaml.safe_load(open('docker-compose.yml'))"
git diff --check
```

At the time this report was prepared, the backend type-check, complete backend test suite, frontend production build, Python compilation, Compose YAML parse, and diff whitespace check passed. The frontend build reports only the existing Vite large-chunk advisory; npm also reports an environment-level `http-proxy` configuration warning.

## Deployment sequence

1. Back up PostgreSQL and the Mosquitto ACL file. Apply migrations `010`, `011`, and `012` before starting the repaired backend/workers.
2. Populate trusted owner grants explicitly. Configure `OWNER_MQTT_USERNAME_MAP` and/or verified `owner_account_nodes` rows. Do not migrate broker logs, denied publishes, or client IDs into verified grants.
3. Confirm the backend can read the ACL file and access the Docker socket needed for Mosquitto `SIGHUP`. Start it with the ACL reconciler enabled and wait for a successful reload log; failures will now retry.
4. Configure `TRUSTED_PROXY_HOSTS`/`TRUSTED_PROXY_IPS` for the actual Nginx peers. Preserve the loopback-only host bindings. Verify the Cloudflare-to-Nginx chain supplies one authoritative `CF-Connecting-IP` value and cannot reach the backend origin directly.
5. Deploy the v3-capable link consumer together with or before new v3 producers. Allow the worker to drain the legacy `meshcore:link_jobs` queue, but do not copy an old backlog into v3. Monitor `meshcore:link_jobs:v3:*`, retry, lease, byte, and dead-letter keys.
6. Start the planned-coverage worker and confirm its heartbeat before enabling planned coverage. Keep backend and worker `PLANNED_COVERAGE_RESULT_TTL_SECONDS` equal.
7. Restart backend/WebSocket processes so old sessions, in-memory caches, proxy peer sets, and live connections cannot retain pre-deployment state. Current owner authorization is still checked server-side after restart.
8. Recompute or allow scheduled refresh of spam and path-learning derived state after deployment so old mixed-scope models are replaced. Do not expose the public path-learning endpoint until migrations and the first successful rebuild complete.
9. Run smoke tests for owner revocation, case-distinct MQTT users, test/production isolation, private node opt-out across REST/WebSocket/exports/timeline/path endpoints, planned-coverage two-caller isolation, proxy IP attribution, queue recovery, and CSV formula neutralization.

## Operational defaults introduced

- Link queue: `LINK_JOB_QUEUE_MAX=5000`, `LINK_JOB_MAX_BYTES=16384`, `LINK_JOB_QUEUE_MAX_BYTES=67108864`, `LINK_JOB_MAX_DELTA=10000`, `LINK_JOB_MAX_RETRIES=5`, `LINK_JOB_DEAD_LETTER_MAX=128`, `LINK_JOB_LEASE_SECONDS=3600` (bounded to six hours).
- Planned coverage: bounded outstanding queue, worker heartbeat requirement, per-request opaque handles, and TTL-backed metadata/results. Keep model version and TTL settings consistent across backend and worker.
- Proxy trust: defaults include loopback plus DNS-resolved `app-ukmesh` and `website-ukmesh`; replace/extend them only with exact expected peers.

## Behavior changes to communicate

- Private nodes are now omitted rather than represented by a stable redacted identity on public collections.
- Packets with malformed path metadata are dropped from public path/timeline/live surfaces.
- `coverage.node_id` for a planned result is the caller's public plan handle, not the shared internal worker job ID.
- Identity-bearing chart responses are recomputed instead of reused from the chart cache; database chart load may increase.
- Public `network=all` is rejected, and legacy production labels map to UKMesh while test remains isolated.
- Frontend/backend origin ports are loopback-only; deployments that previously reached those ports remotely must use the intended proxy/tunnel path.

## Remaining deployment-only checks

These are not unresolved source findings, but they require the target environment:

- Exercise migrations against a production-sized copy and inspect query plans for privacy/path-learning joins.
- Run PostgreSQL fixtures for one-, two-, and three-byte private prefixes plus NULL/malformed `TEXT[]` path elements.
- Run Redis concurrency tests for link claim expiry/recovery and same-coordinate planned-coverage callers.
- Run Mosquitto integration tests covering ACL write, failed reload, retry, and effective publish authorization.
- Verify Cloudflare/Anubis/Nginx/backend HTTP and WebSocket client-IP attribution end to end.
- Load-test the bounded path, stats, spam, planned-coverage, link, and WebSocket limits using production-equivalent replicas.
