# UKMesh Pathing Algorithm — Accuracy Improvement Report

**Date:** 2026-08-02 · **Scope:** read-only analysis (no code changes) · **Author:** Hermes
**Codebase:** `~/meshcore-analytics` on 192.168.100.105 (backend is TypeScript)

---

## 1. Executive summary

The pathing stack is already sophisticated: a Viterbi HMM decoder over prefix-matched candidate trellises, gated by an ITM radio-viability model, boosted by four families of learned priors, an ML model (LightGBM, calibrated, evolutionary-tuned) for 1-byte hash disambiguation, and multi-observer stitching. The fundamental accuracy limit is intrinsic — hops are **truncated Ed25519 pubkey prefixes (1–3 bytes)**, so a 1-byte (2-hex-char) hash can match dozens of nodes.

The biggest accuracy wins are not in the decoder math but in:

1. **Measurement** — the calibration/evaluation loop measures a self-referential proxy on an easy subset, so the confidence numbers (and the UI colour bands) are not proven against true path accuracy.
2. **Silent hard-gates** — links flagged ITM-impossible are excluded even when the network *actually observed* them; real paths get dropped and nobody audits the contradiction.
3. **Multi-observer evidence** — each observer view is solved independently then stitched; a joint inference (position-consistency voting) would extract far more signal from ambiguous hashes.
4. **Calibration granularity** — a single global scale/bias is applied to all evidence classes; unique-3-byte matches and pure-geography guesses get the same calibration.

---

## 2. How the algorithm works today

### 2.1 Wire format (the hard constraint)
- `path_len` byte: upper 2 bits = hash size mode (1/2/3 bytes per hop), lower 6 bits = hop count.
- Path = `hop_count × hash_size` bytes, each hop being the first N bytes of the node's Ed25519 public key. No coordinates in the path. (`multipath.md`)

### 2.2 Two resolvers
| | Lazy (`backend/src/path-lazy/lazyResolver.ts`) | Beta (`backend/src/path-beta/resolver.ts`) |
|---|---|---|
| Role | older/lightweight path view, feed fallback | main live-paths resolver (3D arcs, multi-observer) |
| Candidate source | prefix `LIKE` lookup on nodes, bounded by observer bounding box | context trellis per hop, `MAX_TRELLIS_CANDIDATES_PER_HOP` cap |
| Core | per-position scoring + known-links + anchors | **Viterbi HMM** over trellis (`O(K²·N)`), global optimum, DP + backtrack |
| Hard gate | known-links set (node_links observed ≥2) | **ITM linkPairs only** (`linkPairs` = itm_viable OR force_viable) — anything else is `-Infinity` |
| Priors | prefix/transition/edge/motif + ML (≥0.80) | same families + ML + observer anchors + sticky anchors + affinity |
| Confidence | — | mean hop conf × resolved ratio → calibrated (scale/bias) |

### 2.3 Evidence tiers in the beta decoder (`resolver.ts`)
- **Confirmed tier**: link in `observedLinkPairs` (real traffic) → confidence floor from observation counts (`strongConfirmedFloor`, `confirmedLinkConfidence`).
- **ITM-viable tier**: valid link, no observations → `rawConf` capped at `nonLinkCap` (0.41–0.62) unless multibyte floor wins.
- **Unique multibyte floors**: 4-char single-match ≥ 0.93, 6-char ≥ 0.985 — near-definitive.
- **Ambiguity penalties**: `localPrefixAmbiguityPenalty` (distance/proximity-weighted peers, 2-hop graph check) + linear `(matches−1)·0.01`.
- **Anchors**: direct observer anchors (hop_count → position), sticky anchors with age decay, terminal-collision guard, observer hop hints with typical-hop normalisation.
- **Affinity**: packet-derived neighbour affinity (14-day window, count/observer/SNR/recency, half-life 7 days) + mutual-neighbour Jaccard.
- **ML**: `mlPrefixScores` — LightGBM + `CalibratedClassifierCV` + evolutionary param search, trained only on gold (uniquely-resolved multibyte) data degraded to 1-byte prefixes, no feedback loop. (`ml-path-learner/worker.py`)

### 2.4 Priors + calibration pipeline (`path-learning/rebuild.ts`, hourly via `workers/path-learning.ts`)
- Prefix/transition priors (receiver-region keyed, `MODEL_LIMIT` capped), edge priors (6h hour-bucketed, reliability/directionality/recency/pathloss scored), motif-2/3 priors.
- Calibration: `top1Accuracy = successPackets / evaluatedPackets`, success = ≥60% of a packet's *verified* edges appear in `confirmedLinks`; `scale = clamp(top1/meanConf, 0.55–1.7)`, `bias = clamp(top1 − meanConf·scale, ±0.2)`, `recommendedThreshold = 0.35 + (1−top1)·0.2`.

### 2.5 Multi-observer (`resolveMultiObserverBetaPath`)
- Canonical observation per observer (richest path), each solved independently, shared-prefix stitching + trim, per-observer result list with explanation.

---

## 3. Accuracy gaps found

### A. Measurement & calibration (biggest lever)
- **A1 — Self-referential calibration metric.** `successPackets` counts packets whose verified edges agree with `confirmedLinks` (`rebuild.ts:597-603`) — a proxy, not true path accuracy, and only evaluated on hops that were *uniquely resolvable* (the easy subset). The global scale/bias is therefore optimistic and the UI bands (cyan ≥0.75 / amber 0.4–0.75 / red <0.4) are not validated against ground truth.
- **A2 — Single global calibration for all evidence classes.** A unique 6-char hash (floor 0.985) and a pure-geography fallback hop share the same scale/bias. Per-class calibration is needed: unique-multibyte / ML-dominant / prior-backed / observed-link / geometry-only.
- **A3 — No held-out gold set.** Training data (unique multibyte resolutions) is the same population used for evaluation — leakage risk; `ml-path-learner` correctly avoids feedback loops, but the calibration path does not hold out anything.

### B. Hard gates that silently drop real paths
- **B1 — ITM-impossible is absolute.** `hopScore` returns `-Infinity` when a link isn't in `linkPairs`, and `isImpossibleLink` hard-blocks `pathLoss ≥ impossibleLinkPathlossDb` (`resolver.ts:1126-1129`, `fallback.ts:15-17`) **even when the link has real observed traffic**. If ITM/SRTM is wrong, correct paths vanish with no audit trail. (This class of issue also explains the old UI's "matchedHops excludes ambiguous hops" behaviour.)
- **B2 — No contradiction feedback loop.** No query/log for "observed_count > 0 but ITM says impossible" — that data is the cheapest ground truth for retuning the physical model.

### C. Multi-observer evidence under-used
- **C1 — Per-observer independent solving + stitching** (`resolver.ts:2324+`). A position-consistency matrix (which hash→candidate appears at position P across *all* observers) is only partially used (`globalDirectAnchors` in the lazy resolver). Joint inference would let ambiguous hops be resolved by cross-observer agreement — the single biggest accuracy win available.
- **C2 — hop_count only used for anchors.** The majority hop count across observers is a strong prior on path length that the Viterbi never sees (no length penalty/constraint). It also feeds the UI "remaining hops" figure.
- **C3 — Hash-size mixing.** Observers with 1 vs 2 vs 3-byte paths for the same packet fragment groups; cross-size alignment (a 3-byte hash sharing its first 2 chars with a 1-byte hash) could unify evidence.

### D. Evidence signals not used in the decoder
- **D1 — SNR.** Packets carry SNR; it's used in affinity (`avg_snr`) but not per-hop in `hopScore`. An SNR gradient (candidates closer to the observer should hear it louder) would disambiguate local peers cheaply.
- **D2 — Node role.** `role` is used only for the observer self-prefix guard (`rx.role === 2`, `resolver.ts:1137`). Repeater-vs-end-node role could weight candidates.
- **D3 — Prefix priors lack recency decay.** Edges get `recencyScore`; prefix/transition counts (`rebuild.ts`) are window counts without decay — a node that moved or died keeps a stale prior. Also no day-of-week bucketing (edges/motifs have 6h buckets; prefixes don't).

### E. Prior saturation & weight hygiene
- **E1 — `MODEL_LIMIT` global cap.** Popular prefixes crowd out rare-but-informative priors. Per-prefix top-N would preserve signal.
- **E2 — Duplicated inline weights.** `scoring.ts` header admits `resolver.ts` predates it and still has inline weights — convergence was deferred. The ML learner runs an evolutionary search over its own params; the decoder weights (which are *more* consequential) are hand-tuned with no automated search or A/B.
- **E3 — `ABLATE_LEAKY_PRIORS` exists but no systematic ablation record.** No CI harness measures which evidence family actually moves accuracy.

---

## 4. Recommended improvements (prioritised)

### Phase 1 — Measure honestly (foundation, ~no decoder changes)
1. **Gold-standard evaluation set**: hold out uniquely-resolved 3-byte multibyte packets (say last 14 days, re-sampled weekly). Re-resolve their degraded 1-byte forms; compute **true top-1 path accuracy**, segment accuracy, ECE + Brier.
2. **Per-class reliability curves**: fit calibration (isotonic or platt per class) for: unique-multibyte / ML-dominant / prior-backed / observed-link / geometry-only. Publish reliability diagram + ECE on the stats page.
3. **Fix the calibration success metric** (`rebuild.ts:603`): replace the ≥0.6 proxy with continuous expected-calibration-error on the gold set.

### Phase 2 — Stop dropping real paths
4. **Observed-override tier**: links with `observed_count ≥ N` (e.g. 3) bypass the ITM-impossible hard block (keep a distinct low-confidence marker rather than `-Infinity`).
5. **Contradiction audit worker**: hourly query for observed-but-ITM-impossible pairs; log to a table + surface in health; use the corpus to retune `impossibleLinkPathlossDb` and ITM parameters.

### Phase 3 — Joint multi-observer inference (biggest accuracy win)
6. **Position-consistency voting**: build a `(position, hash) → candidate frequency` matrix across all observers of a packet; use cross-observer agreement as a new trellis-column prior (weight ~ unique-multibyte evidence). This directly attacks 1-byte ambiguity.
7. **Hop-count prior**: soft length constraint in the Viterbi from the majority observer hop count (penalty when decoded length deviates) + expose expected-vs-decoded in the explanation payload.
8. **Cross-size hash alignment** for mixed 1/2/3-byte observer groups.

### Phase 4 — Cheaper decoder-side wins
9. **SNR shaping**: add per-hop SNR-gradient term to `hopScore` (observer-relative).
10. **Recency decay for prefix/transition priors**; optional day-of-week buckets.
11. **Per-prefix top-N** instead of global `MODEL_LIMIT`.
12. **Role-weighted candidates** using the existing `role` field.

### Phase 5 — Keep it from regressing
13. **Converge weights into `scoring.ts`** and make them config-driven; run the ML learner's evolutionary search over decoder weights too (or a simpler random search), with the Phase-1 gold set as the objective.
14. **CI accuracy regression gate**: `path-lazy/evaluate.ts` already exists — wire a fixed gold corpus into CI; any resolver change must not regress top-1 accuracy/ECE beyond a threshold; record ablations (prefix/edge/ML/affinity on/off) to `docs/pathing-ablations.md`.

---

## 5. Verification plan

1. Build the gold set from the DB (unique 3-byte packets, held out).
2. Baseline: current resolver top-1 path accuracy + ECE per class on the gold set (numbers to be recorded in `path_model_calibration`-style table).
3. Apply changes one phase at a time; A/B on the same gold set; publish delta per phase.
4. Live check: monitor `top1`, `mean_pred_confidence`, `recommended_threshold` from the existing calibration row over a week; confirm the UI colour bands now match measured reliability.
5. Spot-check live paths against the map for a sample of packets (manual/hardware evidence step).

---

## 6. Files referenced

- `backend/src/path-beta/resolver.ts` (Viterbi decoder, multi-observer, context)
- `backend/src/path-beta/fallback.ts`, `affinity.ts`, `geometry.ts`, `constants.ts`
- `backend/src/path-shared/scoring.ts` (shared weights — partially converged)
- `backend/src/path-lazy/lazyResolver.ts`, `path-lazy/evaluate.ts`
- `backend/src/path-learning/rebuild.ts` (priors + calibration), `workers/path-learning.ts`
- `backend/src/pathing/pathingService.ts` (+ `pathingRepository.ts`, `pathingPublicDto.ts`)
- `backend/src/platform/config/pathing.ts` (tunables)
- `ml-path-learner/worker.py` (LightGBM 1-byte disambiguator)
- `multipath.md` (wire-format notes)
