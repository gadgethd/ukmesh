# UKMesh Pathing Integration Plan — One Overarching Resolver

**Date:** 2026-08-03 · **Author:** Hermes · **Status:** PLAN ONLY (no code changed)
**Basis:** production code read (backend/src), experiment results (~/multibyte-exp,
REPORT-pathing-experiment.md), prior report PLAN-pathing-accuracy.md.

---

## 1. What exists today (verified in code)

All path resolution flows through **one worker pool** (`path-beta/resolveWorker.ts`)
with three job types, but **two different decoders** behind them:

| job type | entry point | decoder | evidence/weights |
|---|---|---|---|
| `resolveLazy` (API `/pathing` packet path — post-settled full route) | `path-lazy/lazyResolver.ts` (818 ln) | **modern Viterbi** (trellis, marginals→ambiguity, NULL_BASELINE unresolved candidate, observer anchors, observer bounding box) | `path-shared/scoring.ts` (single source of truth) + rebuild priors + ML prefix scores + node_links |
| `resolve` (single-observer view) | `path-beta/resolver.ts` (2913 ln) | **legacy single-observer core** (ln 816–1441): greedy-ish permutation scoring, confidence calibration, fallback modes | **inline weights** (0 refs to SCORE) — explicitly flagged in scoring.ts as "predates this module… converge it here when convenient" |
| `resolveMulti` (multi-observer overlay) | `path-beta/resolver.ts` (ln 2324–2420) | legacy combine: observers solved independently, overlays stitched | inline weights |

Supporting infra, already shared or parallel:
- `path-shared/scoring.ts` — MAX_HOP_KM, SCORE weights, ML_DOMINANT_THRESHOLD, NULL_BASELINE, AMBIG_DELTA, MAX_COL, prior key formats. **Used by lazy only.**
- `path-learning/rebuild.ts` (878 ln) — rebuilds prefix/transition/edge/motif priors + ML prefix scores from packet history.
- `path-lazy/evaluate.ts` (205 ln) — **gold-packet accuracy harness**: multibyte packets with uniquely-resolving hashes → degrade to 1-byte prefixes → compare legacy greedy vs Viterbi; stratifies by prior support to expose leakage. This is the report's Phase-1 eval gate, already in-repo.
- `path-lazy/lazyResolverLegacy.ts` — old greedy (kept for the harness comparison).

## 2. The goal

**One overarching algorithm**: a single Viterbi decode core + one evidence model +
one weights table, powering all three job types. Delete the legacy inline-weight
resolver. One accuracy harness gates everything.

## 3. What the experiment contributes (new signals, validated)

From REPORT-pathing-experiment.md (gold set 75,625 routes; holdout test 11,821):

| signal | gain (test, 1-byte task) | where it plugs in |
|---|---|---|
| **Corridor interpolation** — hop i anchored at i/(N-1) along src→rx line | +9 pp unseen corridors (90.4 vs 81.3) | new emission term |
| **Position-conditional transitions** — spine stability (pos, from)→to | small overall, big on long routes | new transition term |
| **ITM viability bonus** (node_links.itm_viable, 20,363 pairs) | +0.4 pp controlled A/B | transition bonus (bonus-only, never hard-gate) |
| **Tuned weights** (coord-descent: pos_freq 1.2, trans 2.0, obs_dist 80…) | 97.21 → 97.27% overall | scoring.ts values (re-tuned on prod harness) |
| Margin-based per-hop confidence | — | reuse lazy's marginals; feed the beta confidence/3D-arc colouring |

Negative results to NOT repeat: ML candidate scorer (loses to hand-built priors),
elevation transition penalty (regressed), bidirectional decode (neutral),
joint longest-route-fix decode (error propagation — margin-gated version only).

## 4. Target architecture

```
                  ┌─────────────────────────────────────────────┐
                  │          path-core/decoder.ts (NEW)         │
                  │   one Viterbi (max-product) trellis decode  │
                  │   emissions + transitions + NULL_BASELINE   │
                  │   + per-position margins (confidence)       │
                  └──────────────┬──────────────────────────────┘
                                 │ evidence in, path out
        ┌────────────────────────┼─────────────────────────┐
        │                        │                         │
┌───────▼───────┐      ┌─────────▼─────────┐      ┌────────▼────────┐
│ lazyResolver  │      │ beta resolve      │      │ beta resolveMulti│
│ (thin wrapper │      │ (thin wrapper     │      │ (wrapper: group │
│  → canonical  │      │  → single-observer│      │  observers →    │
│  group decode)│      │  decode + DTO)    │      │  canonical      │
│               │      │                   │      │  decode + DTO)  │
└───────┬───────┘      └─────────┬─────────┘      └────────┬────────┘
        │                        │                         │
        └──────────── resolveWorker (unchanged: 3 job types, same DTO contracts)
```

Key decisions:
1. **One decoder, three thin wrappers.** DTO contracts (`pathingPublicDto.ts`,
   `api/contracts.ts`) and the worker pool stay exactly as-is; only the decode
   internals unify. Zero API surface change.
2. **The lazy resolver is the base** — it is already the modern Viterbi with
   shared weights. The legacy path-beta core is deleted after its accuracy is
   matched or beaten by the shared core on the harness (per-job gate, §6).
3. **Multi-observer = canonical grouping.** The lazy resolver already groups
   prefix-compatible observers and decodes once (its `groupByPathHashes`).
   `resolveMulti` adopts this instead of the "solve-then-stitch" legacy combine —
   same packet evidence, one coherent chain, no permutation explosion.
   Preserve the beta DTO fields (permutationCount etc. can be derived or
   deprecated with the frontend's blessing).
4. **All weights in scoring.ts.** Move the beta inline weights out; the SCORE
   table becomes the only tuning surface, re-valued by the harness (§6).
5. **New priors from rebuild.ts** (extend, don't fork):
   - `path_corridor_priors` — (src, rx, pos) → node counts (corridor emission)
   - `path_pos_transition_priors` — (pos, from) → to counts (spine transitions)
   - position-aware prefix counts (`pos_freq`) if rebuild doesn't already carry it
   - ITM viability comes read-only from `node_links.itm_viable` (already computed
     by the link worker; no new compute)
   Lifecycle identical to existing priors (rebuild cadence, generation stamp,
   cache invalidation via visibility generation).
6. **Confidence:** the shared decoder emits per-position margins; lazy's
   AMBIG_DELTA semantics + beta's calibrated confidence/fallback thresholds both
   derive from the same margins. The 3D-arc colouring keeps its current bands.

## 5. Implementation phases (each gated, deployable independently)

- **Phase 0 — Baseline (no code):** run `evaluate.ts` on prod data; capture
  route/hop accuracy for legacy greedy, lazy Viterbi, and (add harness support
  for) the beta single + multi jobs. These numbers are the merge gate.
- **Phase 1 — Extract `path-core/decoder.ts`:** move the lazy Viterbi body out
  of lazyResolver into path-core with an evidence-interface (priors passed in).
  lazyResolver becomes a thin wrapper. Harness must be bit-identical.
- **Phase 2 — Port single-observer `resolve`:** route the beta single-observer
  job through the shared decoder (observer anchor emission gives it the same
  view it has today). Add harness coverage for the beta DTO path. **Delete the
  legacy single-observer core** (resolver.ts shrinks to the multi-observer
  surface) once accuracy ≥ baseline.
- **Phase 3 — Port `resolveMulti`:** canonical grouping (lazy-style) + one
  decode; project per-observer overlays. Compare against legacy combine on the
  harness; keep DTO fields or migrate frontend in the same deploy.
- **Phase 4 — Enable new signals (behind env flags, one at a time):**
  corridor priors → pos transitions → ITM bonus. Each lands with its harness
  delta on the leakage-resistant stratifications (unseen corridors, 13+ hops,
  prior-support buckets). No flag ships enabled without a measured gain.
- **Phase 5 — Re-tune weights:** run the coord-descent tuner against
  `evaluate.ts`'s gold set (real hash sizes — do NOT copy experiment weights
  blindly; they were tuned on 1-byte-degraded data). Commit final SCORE values.
- **Phase 6 — Confidence unification + cleanup:** margin→confidence mapping
  for the 3D arcs; delete `lazyResolverLegacy.ts` if the harness no longer needs
  it; docs in scoring.ts; update multipath.md.

## 6. Acceptance criteria (regression gate, CI-able)

**Accuracy reference: the experiment's vit_src numbers (Ben directive
2026-08-03: "trust your results over anything on the site — they are the most
accurate and the newest").** The harness degrades to the same 1-byte task as
the experiment, so the gates are directly comparable:

`npx tsx src/path-lazy/evaluate.ts` (extended to all three job types) must show:
- aggregate route accuracy ≥ **97.27%** / hop ≥ **99.24%** on the holdout-style
  stratification (unseen-corridor and 13+ buckets tracked separately, matching
  the experiment: ≥ 93% unseen, ≥ 89% 13+);
- shared decoder ≥ legacy decoder on **every** stratification (length buckets,
  seen/unseen corridors, prior-support buckets), no single regression > 0.5 pp;
- API DTO parity tests (`pathing.test.ts`, `pathingPublicDto.test.ts`) pass;
- no new endpoints; worker pool untouched; rebuild runtime within budget.

## 7. Risks & mitigations

| risk | mitigation |
|---|---|
| Corridor/pos priors memorize repeated corridors (leakage) | evaluate.ts "supported" stratification + margin-gated usage; cap table size (top-N per key) |
| Prior table size blow-up (corridor_pos ~ src×rx×pos) | caps (MAX keys, top-N per key), same retention as existing priors |
| DTO drift breaking the map frontend | DTO contracts frozen in Phase 1-3; frontend verify on staging; own-eyes screenshot check per Ben's policy |
| Worker thread memory with bigger priors | MAX_COL already caps trellis; priors loaded per-generation with LRU |
| Harness regressions hidden by leakage | all gates use the leakage-resistant stratifications |
| Live-paths visual regression | deploy → verify live-paths + main map manually (screenshots) before merge |
| VM resource load during rebuild/tune | reuse run_capped discipline: 4 cores, nice, memory guard (from the experiment) |

## 8. Effort & sequencing (estimate)

- Phase 0: 0.5 day (harness extension + baseline capture)
- Phase 1: 0.5–1 day (extract, no behavior change)
- Phase 2: 1 day (port + delete legacy single-observer core)
- Phase 3: 1–1.5 days (canonical multi-observer + DTO migration decision)
- Phase 4: 0.5 day per signal (flag + harness delta)
- Phase 5: 0.5–1 day (retune on prod gold set)
- Phase 6: 0.5 day (confidence + cleanup + docs)
Total: ~4–6 focused days, each phase independently deployable. Can be run as a
MissionDeck wave (agents per phase, local commits, no deploys) with Hermes
verifying the harness gates between phases — or direct, per Ben's preference.

## 10. Ben's decisions (2026-08-03, binding)

1. **All implementation on the ukmesh VM (192.168.100.105)** — nothing local.
   Agents run via MissionDeck `codex@ukmesh` backend in git worktrees under
   `/home/ben/worktrees/pathing-*` on that host; canonical repo untouched
   (worktrees branch from HEAD 8d218c1; the single-repeater-coverage WIP in the
   main checkout is NOT touched by this wave).
2. **Fallback red paths: REMOVED.** One set of paths, all blue. The unified
   decoder's NULL_BASELINE handles unresolved hops (they stay unrendered or
   flagged) — no fallback rendering mode.
3. **resolveMulti: canonical-path projection** (Hermes decision, Ben delegated):
   lazy-style prefix-compatible observer grouping + ONE coherent Viterbi decode;
   per-observer overlays projected from the canonical path. Kills the legacy
   solve-then-stitch + permutation explosion.
4. **Execution: one MissionDeck agent wave** (staged: 2 foundation agents →
   3 parallel → 1 integration/tuning agent), as many agents as needed. Hermes
   orchestrates, verifies gates between stages, merges; agents commit locally,
   never push/deploy/restart services.
5. **Accuracy bar: the experiment's vit_src numbers** (97.27% route / 99.24%
   hop test; ≥93% unseen corridors, ≥89% 13+ hops) via the extended harness.

## 11. Wave status (2026-08-03)

- Stage 1 (IN FLIGHT): pathing-core-extract (sol/xhigh) + pathing-eval-harness
  (luna/max) on ukmesh worktrees.
- Stage 2 (QUEUED): beta single-observer port (sol/xhigh) | multi-observer
  canonical + fallback removal backend (sol/xhigh) | frontend all-blue
  (luna/max).
- Stage 3 (QUEUED): new signals (corridor/pos-trans/ITM) in rebuild.ts +
  decoder (sol/xhigh), then weight tuning + confidence + cleanup (sol/xhigh).


