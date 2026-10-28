# L3 UKMesh privacy fence — 2026-10-28

Branch: `burn/ukmesh-privacy-fence-20261028`, created after fetching `origin/main`
at `5a3c9445aab0dcd540b0def01b1425d935c2e24f`. The first branch commit,
`1aa404d95d734a994fac4577852d7d5d437e118a`, contains only the failing regression.
The subsequent implementation commits are `cf6fdda9`, `a7fa00b3`, `81d0c24f` and `cf81568c`
(`git log origin/main..burn/ukmesh-privacy-fence-20261028`). PR: pending creation.

## Problem and root cause

The owner ACL readiness fence is distinct from packet-history visibility
materialization. PR #149 already admitted verified feed-only permission changes,
but rejected an **empty** permission delta when generation IDs differed. Its
classifier returned `rendered-acl-policy-unchanged`, while the validator returned
`ok: false` and three generation mismatches. The new regression reproduces that
case with an out-of-band settled generation and matching applied-file marker
(`backend/src/owner/ownerInventoryReadiness.test.ts:80`; captured failure in
`evidence/2026-10-28-l3-ukmesh-privacy-fence/before.tap:1`).

The previous generation hash described render inputs rather than the whole
emitted policy: unused options could change the ID, while retained unmanaged
permissions could change without a new ID. The previous apply loop also rewrote
and reloaded unchanged policies. Compare the pre-fix implementations at
`1aa404d9:backend/src/mqtt/aclManager.ts:218` and
`1aa404d9:backend/src/owner/ownerAclReconciler.ts:108` with the corrected paths
below. Related history reviewed: `0679fe19`, `04486cad`, `69cc48a7`, and PRs
[#149](https://github.com/gadgethd/ukmesh/pull/149) and
[#159](https://github.com/gadgethd/ukmesh/pull/159).

## Change and invariants

- Hash the emitted directives and user scopes, including retained global and
  unmanaged rules. Comments, markers and unused options do not affect policy
  identity (`backend/src/mqtt/aclManager.ts:260`; regressions at
  `backend/src/mqtt/aclManager.test.ts:16` and `:28`).
- A fresh authoritative render, an applied artifact with the expected renderer,
  generation, hash and exact bytes, current applied state, and a fresh file
  readback permit a no-op. No state, artifact, shadow-file or ACL write or broker
  reload occurs. A desired/rendered-only bump repairs the state once; repeating
  the pass is a no-op (`backend/src/owner/ownerAclConvergence.ts:69`; tests at
  `backend/src/owner/ownerAclConvergence.test.ts:65` and `:74`).
- Other drift takes the existing validated apply/reload/readback path. A pending
  error fence precedes side effects, and the complete generation triple is
  published only after verification. Reload/readback failures restore the prior
  file and leave readiness fenced (`backend/src/owner/ownerAclConvergence.ts:94`;
  tests at `backend/src/owner/ownerAclConvergence.test.ts:152` and `:167`).
- Startup still awaits the same reconciler before the HTTP listener opens.
  In apply mode, an unsettled readiness observation requests that pass,
  coalesces concurrent probes, throttles attempts to ten-second intervals, and
  re-reads state. Existing periodic and public-feed reconciliation remain in use
  (`backend/src/owner/ownerAclReconciler.ts:63`, `:73`, `:147`;
  `backend/src/owner/ownerAclConvergence.ts:15`; `backend/src/index.ts:417`;
  concurrency regression at `backend/src/owner/ownerAclConvergence.test.ts:201`).
- Readiness accepts verified identical policies as well as the existing
  public-feed-only delta. Inventory counts/grants/configuration, non-feed
  directives, settled states, artifact hashes/markers, renderer identity and
  live-file readback retain their checks. Inventory exports remain strict; no
  baseline file is overwritten (`backend/src/owner/ownerInventoryReadiness.ts:81`;
  `backend/src/owner/ownerInventoryBaseline.ts:142`; negative regressions at
  `backend/src/owner/ownerInventoryReadiness.test.ts:105`, `:158`, `:174`, `:188`,
  `:205`). The readiness generation gate now also checks rendered generation
  (`backend/src/index.ts:448`).
- Artifact persistence replaces stale renderer metadata and preserves an applied
  timestamp only for the same validated bytes, hash, renderer and apply mode.
  A changed render cannot inherit an older application certificate
  (`backend/src/owner/ownerAclArtifacts.ts:39`; actual SQL regressions at
  `backend/src/owner/ownerAclArtifacts.integration.test.ts:24`, `:35`, `:51`).

The hash transition changes IDs once even where permissions are unchanged.
Historical applied artifacts and the existing baseline must remain available so
readiness can prove equivalence. The renderer identifier remains v5 because the
permission syntax is unchanged; changed/missing renderer evidence still rejects
the exception (`backend/src/mqtt/aclManager.ts:6`, `:260`;
`backend/src/owner/ownerInventoryReadiness.ts:98`). Missing historical evidence
cannot safely be invented from a baseline containing only owner-node readback.

## Reproduction: failed before, passed after

Run from `backend/` after installing its committed dependencies and creating
`.ukmesh-tools/tmp`:

```sh
TMPDIR="$PWD/.ukmesh-tools/tmp" node --import tsx --test \
  --test-name-pattern='spurious out-of-band ACL generation' \
  src/owner/ownerInventoryReadiness.test.ts
```

Before (`1aa404d9`, exit 1; full output in
`evidence/2026-10-28-l3-ukmesh-privacy-fence/before.tap:1`):

```text
not ok 1 - spurious out-of-band ACL generation converges when verified applied policy is unchanged
# OUT-OF-BAND GENERATION: strict baseline=false; readiness=false; reason=rendered-acl-policy-unchanged
# tests 1
# pass 0
# fail 1
```

After (`81d0c24f`, exit 0; full output in
`evidence/2026-10-28-l3-ukmesh-privacy-fence/after.tap:1`):

```text
ok 1 - spurious out-of-band ACL generation converges when verified applied policy is unchanged
# OUT-OF-BAND GENERATION: strict baseline=false; readiness=true; reason=rendered-acl-policy-unchanged
# tests 1
# pass 1
# fail 0
```

The separate state-drift regression asserts the gate is false before recovery,
true after the readiness-triggered authoritative pass, zero broker reloads and
an unchanged repeat. The startup-pass regression repairs a settled spurious
generation and file marker (`backend/src/owner/ownerAclConvergence.test.ts:74`,
`:92`). Feed enable/disable cycles return to the original canonical generation;
non-feed permission changes still reject readiness against the old baseline
(`backend/src/owner/ownerAclConvergence.test.ts:114`).

## Verification

Commands below ran from `backend/`. Exact output excerpts are retained in
`evidence/2026-10-28-l3-ukmesh-privacy-fence/verification.txt:1`.

```sh
TMPDIR="$PWD/.ukmesh-tools/tmp" npm test
TMPDIR="$PWD/.ukmesh-tools/tmp" node --import tsx --test \
  src/owner/ownerAclConvergence.test.ts \
  src/owner/ownerInventoryReadiness.test.ts \
  src/owner/ownerInventoryBaseline.test.ts \
  src/owner/autoRebaseline.test.ts src/mqtt/aclManager.test.ts \
  src/api/routes/ownerPublicFeed.test.ts
npm run typecheck
npm run build
git diff origin/main --check
```

Results: full backend command **736 passed, 0 failed, 3 skipped** (739 total),
including its alert-receiver smoke script; focused suite **49 passed, 0 failed,
0 skipped**. Typecheck, build and whitespace check exited 0. The three full-suite
skips are opt-in live read-only EXPLAIN tests; no live database was used
(`evidence/2026-10-28-l3-ukmesh-privacy-fence/verification.txt:1`).

Actual artifact repository SQL also ran against the real owner-auth schema in
an in-memory PostgreSQL-WASM fixture: **4 passed, 0 failed, 0 skipped**
(`evidence/2026-10-28-l3-ukmesh-privacy-fence/artifact-sql.tap:1`). Reproduce with:

```sh
npm install --prefix .ukmesh-tools/pglite --no-save --package-lock=false @electric-sql/pglite@0.5.12
TEST_OWNER_ACL_PGLITE_MODULE="file://$PWD/.ukmesh-tools/pglite/node_modules/@electric-sql/pglite/dist/index.js" \
TMPDIR="$PWD/.ukmesh-tools/tmp" node --import tsx --test \
  src/owner/ownerAclArtifacts.integration.test.ts
```

This fixture opens no server connection. Convergence tests inject all state,
file and broker side effects (`backend/src/owner/ownerAclArtifacts.integration.test.ts:7`;
`backend/src/owner/ownerAclConvergence.test.ts:16`). No production deployment,
live DB access, migration run, live configuration edit or service restart was
performed. No repair endpoint was needed or added; runtime recovery uses the
existing reconciler (`backend/src/owner/ownerAclReconciler.ts:63`).

## Deliberately left out and ranked follow-ups

1. **Release validation:** retain baseline/applied-artifact evidence, then use
   the normal separately authorized release process and verify readiness,
   toggle behavior and broker readback. Production behavior was not observed
   during this PR-only task; the local proof uses injected ports and in-memory
   SQL as described above.
2. **Cross-process coordination:** the existing single-flight lock serializes
   one process, not multiple independent ACL writers. If multiple replicas are
   introduced, add shared writer coordination before relying on that topology
   (`backend/src/owner/ownerAclReconciler.ts:60`, `:73`).
3. **Optional fixture in CI:** consider pinning the PostgreSQL-WASM module in
   CI and running the artifact SQL suite there. Today it is explicitly opt-in;
   ordinary `npm test` excludes integration files (`backend/package.json:27`;
   `backend/src/owner/ownerAclArtifacts.integration.test.ts:8`).
4. **Separate packet-history fence:** no automatic full-history
   `privacy:converge --apply`, queue-claim reset, migration renumbering, policy
   choice change or packet rewrite was added. PR #159's read-only packet
   generation monitor and the existing reviewed repair procedure remain
   separate (`backend/src/privacy/generationHealth.ts:79`;
   `docs/privacy-convergence.md:123`). This task repairs the owner ACL generation
   and `/readyz` baseline failure described by the brief.

Evidence SHA-256:

```text
3593857c8e1cc67dc53e811bf11ba5a5ecafb4d611664f23e125e0496ea99518  before.tap
1d9be6113e180ea0a67219028cb560a10cab83e5692fa35e40bd643657564d68  after.tap
5d49ca4a562d8ea66779cf776b6b769cd881b318500edfb34fdbd7e5b849536a  artifact-sql.tap
```

PROOF: PR pending; regression command above: before `# pass 0` / `# fail 1`,
after `# pass 1` / `# fail 0`; complete before/after TAP and SHA-256 values above.
