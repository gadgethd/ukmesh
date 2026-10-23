# UKMesh release notes — 23 October 2026

Coverage: the five fixes merged on 22 October 2026 (#141–#145) and the three requested PRs opened on 23 October 2026 (#149–#151).

Status checked against GitHub on 2026-10-23 16:46 UTC. PR titles and every PR commit subject below are reproduced verbatim; commit links contain full SHAs. Summaries use the linked PR descriptions and implementation commits. Repository merge status does not establish production deployment.

## Merged on 22 October 2026

### [#141 — Fix rotating-ID spam gaps and stale campaign incident rows](https://github.com/gadgethd/ukmesh/pull/141)

Merged **2026-10-22 21:12:32 UTC**; merge commit [dc4ea77b](https://github.com/gadgethd/ukmesh/commit/dc4ea77be19e13ad14f8e77031ef4d6515c95fea).

Detects qualifying rotating-ID message bursts at six transmissions while retaining the ordinary eight-message floor. Raises the default analysis budget from 2,000 to 5,000 messages with truncation warnings, merges recurring campaign bursts, preserves historical first-seen and newer evidence, and honors small evidence budgets.

PR commits (exact subjects, including documentation and branch synchronization):

- [d9461c2c](https://github.com/gadgethd/ukmesh/commit/d9461c2c1cab2ed0965ecf84e4a319023725cd29) — fix(spam): detect small rotating-ID bursts and preserve campaign evidence
- [ef5f6ea7](https://github.com/gadgethd/ukmesh/commit/ef5f6ea7995cd0e5f01c544b9713e3e8bd1cfc0b) — docs: record rotating-ID spam fix verification and PR
- [d4e07aa9](https://github.com/gadgethd/ukmesh/commit/d4e07aa9f628763a7176326ea9e053a6d192b63b) — fix(spam): honor small campaign evidence budgets
- [f11fbe10](https://github.com/gadgethd/ukmesh/commit/f11fbe10e980b3e5876b2f6f84f82b4b07b6920b) — docs: record continuation audit and evidence-cap proof
- [1ed3d473](https://github.com/gadgethd/ukmesh/commit/1ed3d4732d6013b7fb952b1274c9be509291c7ce) — Merge remote-tracking branch 'origin/main' into burn/spam-rotating-ids-20261022

### [#142 — perf(api): aggregate MQTT receiver counts before canonicalization](https://github.com/gadgethd/ukmesh/pull/142)

Merged **2026-10-22 21:05:38 UTC**; merge commit [b989043e](https://github.com/gadgethd/ukmesh/commit/b989043ee939a805ca3c80805a1af82ad83bf15d).

Speeds up `GET /api/mqtt-nodes` by aggregating raw receiver counts before canonicalizing identities, preserving bigint counts, response fields, ordering and network scope. The PR reports local HTTP-harness p50 of 8,775.144 ms → 548.563 ms using read-only VPS queries; this measures the proposed code. The change adds no response cache or migration.

PR commits (exact subjects, including documentation and branch synchronization):

- [eef1244e](https://github.com/gadgethd/ukmesh/commit/eef1244eca884b4f03c12775c2e2e9fcb47895a8) — perf(api): aggregate MQTT receiver counts before canonicalization
- [f95ad420](https://github.com/gadgethd/ukmesh/commit/f95ad42020c7858830667faf9c545c079f38c41e) — docs: record MQTT nodes performance proof and review
- [02c2699c](https://github.com/gadgethd/ukmesh/commit/02c2699c5cc886820de246e4e79b36e2dc58bfa1) — docs: verify MQTT performance handoff against current PR
- [ce54edf5](https://github.com/gadgethd/ukmesh/commit/ce54edf5e54e448528f58b8d84ca023f565ffe55) — docs: refresh MQTT performance completion evidence

### [#143 — Fix empty prefix cascade fences and add verified privacy convergence](https://github.com/gadgethd/ukmesh/pull/143)

Merged **2026-10-22 21:12:35 UTC**; merge commit [0a8cb97e](https://github.com/gadgethd/ukmesh/commit/0a8cb97e4d493745e53a9e330afad19765a56560).

Migration 062 ignores empty private-prefix cascade statements so deleting a public node does not spuriously close materialized public reads. Real prefix changes and TRUNCATE retain fail-closed invalidation, and existing generation gaps are preserved. Adds `privacy:converge`, read-only by default, with explicit generation-checked apply, historical reclassification and atomic publication. Apply requires a maintenance window; production lock/WAL/runtime cost remains unmeasured.

PR commits (exact subjects, including documentation and branch synchronization):

- [97a007af](https://github.com/gadgethd/ukmesh/commit/97a007af1098906653c8a5d8be354f0f3c3789d2) — fix(privacy): ignore empty prefix cascades and add verified convergence
- [03d9692e](https://github.com/gadgethd/ukmesh/commit/03d9692efd021f48b1997e09658e8a1faa03b50d) — docs: record privacy fence proof, rollback and PR handoff
- [d456bda0](https://github.com/gadgethd/ukmesh/commit/d456bda0fcc6f4ddeea825b95b46c2813b383470) — docs: verify privacy fence handoff and completed CI
- [c1087b60](https://github.com/gadgethd/ukmesh/commit/c1087b601007afe3fd4e8a6792e14d5ad1c113c4) — Merge remote-tracking branch 'origin/main' into burn/privacy-fence-20261022

### [#144 — docs(rescue): audit deploy-tree recovery and map remaining drift (#535)](https://github.com/gadgethd/ukmesh/pull/144)

Merged **2026-10-22 21:12:29 UTC**; merge commit [1cc06518](https://github.com/gadgethd/ukmesh/commit/1cc06518df89f92fc466dc42d3e33bb394de5741).

Adds the 221-path deploy-tree rescue ledger and an offline Git-object verifier, documenting that merged PR #133 already preserved the readable feature work and all five VPS-local commits. This audit introduces zero additional application source files. The local liveness override and VPS conflict-index repairs remain parked; the ledger does not establish deployed-image provenance.

PR commits (exact subjects, including documentation and branch synchronization):

- [d6a2759b](https://github.com/gadgethd/ukmesh/commit/d6a2759bcd0a34c662109c1db989868bdb9eb16d) — docs(rescue): prove deploy-tree features already landed on main
- [97d5111e](https://github.com/gadgethd/ukmesh/commit/97d5111e6c35479e22c84a67859aa0ebb2cf468d) — docs(rescue): record file ledger, verification and parked deployment gaps
- [836af3ac](https://github.com/gadgethd/ukmesh/commit/836af3ac429ac187efc5badbd0114c7b01748feb) — docs(rescue): verify completion evidence and finished checkpoint CI
- [2f756be7](https://github.com/gadgethd/ukmesh/commit/2f756be7c772efd51bfaaf77214926686cbcf07a) — Merge remote-tracking branch 'origin/main' into burn/deploy-tree-rescue-20261022

### [#145 — fix(health): align vacuum backlog with PostgreSQL triggers](https://github.com/gadgethd/ukmesh/pull/145)

Merged **2026-10-22 21:13:12 UTC**; merge commit [73ab7d4f](https://github.com/gadgethd/ukmesh/commit/73ab7d4fc63e4968a5fd46af022696ad69d16d9b).

Aligns vacuum-backlog health reporting with effective server/per-table PostgreSQL autovacuum triggers while retaining the 10,000-dead-row noise floor and table-count severity. Adds read-only diagnostics and SQL regression coverage. The reported snapshot changes qualifying tables from five to zero because they were below their vacuum triggers; this change performs no VACUUM and requires no migration.

PR commits (exact subjects, including documentation and branch synchronization):

- [767f37e0](https://github.com/gadgethd/ukmesh/commit/767f37e04211ac6c281ba07b59b8dfe7b6b02a90) — fix(health): align vacuum backlog with PostgreSQL triggers
- [013ae692](https://github.com/gadgethd/ukmesh/commit/013ae692591eabc46b718dd1ce3ca27be6377886) — docs: record vacuum diagnosis handoff and PR
- [92b71781](https://github.com/gadgethd/ukmesh/commit/92b717812a05cac3e9d250f4eb8e05c907bbd5cb) — Merge remote-tracking branch 'origin/main' into burn/vacuum-backlog-20261022
- [c39452eb](https://github.com/gadgethd/ukmesh/commit/c39452eb8f66870dd7be7848201cf8ed9d542129) — Merge remote-tracking branch 'origin/main' into burn/vacuum-backlog-20261022

## Open PRs from 23 October 2026

These three changes remain unmerged at the status check above.

### [#149 — fix: exempt verified public-feed-only ACL drift from readiness](https://github.com/gadgethd/ukmesh/pull/149)

**Open, unmerged**; branch head [a5bfa546](https://github.com/gadgethd/ukmesh/commit/a5bfa546af220804ce76ec7dd99b27a7671daf7f).

Proposes allowing `/readyz` baseline drift only when applied ACL artifacts prove that all rendered differences are additions/removals of `topic read public/+/+/packets`. Inventory changes, other permission changes, unsettled generations and missing/mismatched evidence still fail closed; strict inventory validation remains unchanged.

PR commits (exact subjects, including documentation and branch synchronization):

- [526adac9](https://github.com/gadgethd/ukmesh/commit/526adac9af1b32293f887411bb3731b46fa8f46e) — fix: tolerate verified public-feed-only ACL drift in readiness
- [a5bfa546](https://github.com/gadgethd/ukmesh/commit/a5bfa546af220804ce76ec7dd99b27a7671daf7f) — docs: record readiness ACL exception evidence and handoff

### [#150 — ci: retry transient registry and apt build failures](https://github.com/gadgethd/ukmesh/pull/150)

**Open, unmerged**; branch head [ec51e459](https://github.com/gadgethd/ukmesh/commit/ec51e459dc9c3e06a0a29f7a5d9806ea7234cc29).

Proposes bounded retries for recognized transient registry and apt acquisition failures, with up to three attempts and 5s/10s backoff. Preloads pinned images, makes apt index errors strict and uses BuildKit cache when retrying failed apt layers. Application builds and test gates keep their failure behavior; Playwright apt installation is retried separately.

PR commits (exact subjects, including documentation and branch synchronization):

- [5312b342](https://github.com/gadgethd/ukmesh/commit/5312b342e92eede57d6d51846e7d4e5c426e7215) — ci: retry transient image and apt failures without replaying test gates
- [ec51e459](https://github.com/gadgethd/ukmesh/commit/ec51e459dc9c3e06a0a29f7a5d9806ea7234cc29) — docs: record CI retry verification and handoff

### [#151 — Surface MQTT monitor failures and bound ingest SQL](https://github.com/gadgethd/ukmesh/pull/151)

**Open, unmerged**; branch head [a542111f](https://github.com/gadgethd/ukmesh/commit/a542111fc7656be203c3a19e68230038750a6a77).

Proposes surfacing Mosquitto log-monitor failures through `/readyz`, metrics and the existing readiness alert. Bounds ingest SQL with a dedicated two-connection pool, a 10-second server timeout and an 11-second client fallback, with rollback and failed-socket disposal. Includes exactly 90% disk usage in the alert and updates the RAM shutdown fixture to observe the dedicated pool. Current spam-node activity remains unverified without a fresh offline snapshot.

PR commits (exact subjects, including documentation and branch synchronization):

- [0ada4674](https://github.com/gadgethd/ukmesh/commit/0ada4674b376bf62f5352ea50c296c9a08729bb0) — fix(ingest): surface monitor failures and bound SQL execution
- [548b541c](https://github.com/gadgethd/ukmesh/commit/548b541cd9d37d05bd76690cc3f87229a1b74989) — docs(review): record ingest fixes, verification and activity evidence gap
- [03dd07c6](https://github.com/gadgethd/ukmesh/commit/03dd07c63d24c307ef7ab5ed46d78e19e83c65a6) — test(ingest): observe dedicated pool in RAM shutdown gate
- [a542111f](https://github.com/gadgethd/ukmesh/commit/a542111fc7656be203c3a19e68230038750a6a77) — docs(review): record green CI and RAM gate follow-up
