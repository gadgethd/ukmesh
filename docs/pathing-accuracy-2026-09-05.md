# Path accuracy verification — 5 September 2026

The revised resolver is more accurate than both production algorithms at
commit `1eefb50b31fda8d79ef383a658764671c65cb46a` on the same held-out three-byte
flood observations degraded to one-byte prefixes. All production path views
now use the revised evidence provider.

## Final results

The evaluation contains **1,377 packets, 9,190 relay positions and 1,292 unique
relay chains**. Entire-route accuracy requires every relay identity to match;
unresolved positions count as errors.

| Resolver | One-byte exact routes | One-byte correct hops | Original three-byte routes preserved |
| --- | ---: | ---: | ---: |
| Previous beta, single observer | 34.79% (479) | 68.72% (6,315) | 1,165 / 1,377 |
| Previous detail resolver, single observer | 75.16% (1,035) | 90.87% (8,351) | 1,184 / 1,377 |
| Revised, single observer | **82.14% (1,131)** | **94.78% (8,710)** | **1,377 / 1,377** |
| Previous beta, multiple observers | 35.22% (485) | 68.75% (6,318) | 1,142 / 1,377 |
| Previous detail resolver, multiple observers | 78.29% (1,078) | 93.76% (8,617) | 1,246 / 1,377 |
| Revised, multiple observers | **82.43% (1,135)** | **94.84% (8,716)** | **1,377 / 1,377** |

Against the stronger previous multi-observer implementation, the revision
corrects **71 routes** and regresses **14**: a net gain of 57 routes, or
**19.1% fewer incorrect routes**. It corrects 99 additional relay positions.
The route gain is 4.14 percentage points; a deterministic bootstrap giving
each unique relay chain equal weight yields a 95% interval of **+3.02 to
+5.80 percentage points** for that unique-chain metric. This interval is not
an estimate of accuracy on every real one-byte packet.

The multi-observer gain appears in every reported route-length and source
stratum. On 104 routes with at least 13 relays, exact matches rise from 44 to
60. On 1,082 packets without a known source, they rise from 856 to 901.
Some individual cases still regress; one-byte identities remain ambiguous.

[Machine-readable aggregate results](pathing-accuracy-2026-09-05.json) include
all paired comparisons, counts, intervals and strata.

## What changed

- The live beta resolver previously omitted the learner's position-prefix,
  corridor and position-transition tables and used transition probabilities
  where the scoring weights expected counts. Those signals now reach the
  shared decoder, including directed source and receiver edges.
- Candidate bounds grow with route length. Long flood routes no longer lose
  their distant first relays to an observer-centered, one-hop bounding box.
- Unique multibyte identities survive missing coordinates and contradictory
  terrain estimates. Repeated, verified directed training edges can override
  geographic rejection of inferred links. Unpositioned inferred candidates
  need directed link evidence and retain null coordinates.
- Mixed hash widths index every matching candidate, preserving elevation.
  Forward/backward passes cache transition scores to keep inference bounded.
- RX prefix collisions no longer remove a real terminal relay. Direct-routing
  instructions are excluded from observed flood paths. Invalid hashes cannot
  shift later positions. One-byte processing supports up to 63 hops.
- Distinct flood copies heard by one observer are retained. Confirmed branches
  stay separate; observer-specific projections stop at their own receivers.
- Map labels use each branch's identities, so co-located relays cannot borrow
  another branch's label or collapse distinct decoded routes by coordinates.
  A relay sharing the source's byte prefix no longer removes that source
  endpoint from the projected path.
- Late evidence can refine any position in a held chain. Holding a choice
  does not manufacture perfect confidence.
- Packet details, overlays, slow finalization and history batches share the
  decoder and evidence provider. Historical UKMesh network labels remain in
  scope. Public visibility checks remain in place.
- Training preserves gaps and verified loops, excludes direct-routing paths,
  and uses RX/TX metadata. A missing relay cannot create a learned shortcut.

## Method and reproducibility

The read-only, repeatable-read snapshot was captured at
**2026-09-05 01:18:07.69378 UTC**, over the preceding 14 days. It includes
14,542 public node records, 155,581 link records, the published prior tables
and 39,786 observations belonging to 3,489 candidate held-out packet hashes.
UKMesh's historical `northeast` and `teesside` labels are included.

Snapshot SHA-256:

```text
f92b5be6776f589ab3c237875c449989545ba56a6c15d9e0ec220b6cf2047632
```

The running learner used the default PostgreSQL hash buckets 0–69 for
training; its environment had no training-split override. Buckets 70–84 were
used for development and 85–99 for final evaluation. Both splits are disjoint
from training at packet-hash level, including every observer's copy of the
packet. The geographic and learned weights were not tuned on final results.

Ground truth requires a three-byte **flood** route, uniquely matching public
node identities and a positioned observer. Nodes without current coordinates
remain eligible as known identities. Direct routes, missing/colliding node
identities and paths without sufficient usable truth exclude 410 of the
1,787 final candidate packet hashes. There is no distance-based exclusion.
This sample covers 2–21 relay positions; longer one-byte paths are supported
and tested structurally but cannot have equivalent three-byte paths beyond
the protocol's path-byte limit.

Every one-byte replay truncates all visible sibling observations, not just the
target path. Multi mode admits siblings within 60 seconds on either side of
the selected observation. Both algorithms receive the same snapshot and
observation window. The archived baseline changes only the export visibility
of its existing packet-preparation helper, not its implementation or scores.
The former detail resolver's SQL adapter is replayed against the frozen
snapshot. Node IDs, rather than rendered coordinates, determine correctness.

This is **packet-held-out validation using current metadata**, not a temporal
forecast or a recreation of past coordinates. Recurrent corridors may occur
in both training and evaluation. Learned priors are frozen identically for
both algorithms, and unique-chain bootstrap results reduce the influence of
recurrent route copies. It does not establish that every live one-byte path,
or every confidence value, has the reported accuracy.

The local snapshot, per-packet results, comparisons, source hashes and test
logs are under `artifacts/path-accuracy/2026-09-05-final/` (ignored by Git).
The aggregate JSON linked above is tracked. [Pathing documentation](pathing.md)
contains capture, baseline archive, replay and comparison commands. The older
`path-lazy/evaluate.ts` is marked as historical diagnostics because its
original sampling did not enforce held-out membership.

## Runtime and validation

The revised offline decode measured **10.62 ms median, 30.30 ms p95 and
42.80 ms p99** in multi-observer mode. Single-observer p95 was 30.75 ms.
These measure decoding against a loaded context, including observation
preparation; they exclude database loading and network latency. Baseline lazy
replay timings include its snapshot-query adapter and should not be used as
a production latency comparison.

A separate smoke test ran the actual database-backed single, multi and detail
resolver entry points with PostgreSQL enforcing
`default_transaction_read_only=on`. One-byte and three-byte samples produced
all three responses, with correct observer endpoints. Warm detail requests
were approximately 287 ms and 500 ms; the first request loading context took
3.23 seconds. These are two samples under concurrent test load, not latency
percentiles. The existing worker pool, context caching and propagation
settling window remain in use.

Validation completed:

- Backend TypeScript build/typecheck and frontend TypeScript check passed.
- **56 path, learning, cache, worker, DTO, API and accuracy-harness tests passed.**
- **9 relevant frontend path/rendering tests passed.**
- Full backend run: **336 passed, 3 failed** in pre-existing, untracked files.
  `analysis/runState.test.ts` imports a missing `normalizeRetiredAnalysisState`;
  `repositories/networkAnalysis.test.ts` expects an unprovided abort signal;
  `api/routes/rfValidation.test.ts` waits indefinitely for that signal and its
  test process was terminated after confirming the failed assertion. These
  files were present before this work and were not modified.
- Database-backed smoke tests were read-only; no prior tables were rebuilt.

Deployed live on **2026-09-05 at 02:03 UTC**. The backend, path-learning worker,
and app now run the reviewed path implementation. Both `ukmesh` and `all`
models have published `path-learning-v5-contiguous` priors. The accuracy comparison
above still uses the same frozen priors for both implementations.

Production verification passed: six single/multi/detail API checks across public
one-byte and three-byte packets, all eight identities in the sampled three-byte
route, 64 served JS/CSS assets, public app readiness and HTML, and live packet
streaming through the app proxy. The private-packet probe returns 404. The API and
app are healthy, with no container restarts or OOM events after the final rollout.
Other running containers retained their original IDs.

The rollout exposed an August owner-inventory snapshot that the current readiness
check rejected. A fresh inventory was validated against the unchanged pre-deploy
broker policy and pinned; the original snapshot was preserved. An initial smoke
probe accidentally selected a private packet and triggered the deployment guard's
rollback. Public samples were then checked in the release image before the
successful retry. No access grants were changed.

Image IDs, checks, exact build-source archive, and rollback references are recorded
in [the local deployment evidence](../artifacts/path-accuracy/2026-09-05-deploy/completion.json)
and its adjacent `manifest.json`. Previous service images remain tagged for rollback.
