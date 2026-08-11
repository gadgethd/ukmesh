# UKMesh Pathing Algorithm Experiment — Final Report

**Date:** 2026-08-03 · **Location:** ~/multibyte-exp/ on the Hermes VM
**Goal:** store all resolvable multibyte routes as ground truth, then build the
most accurate path-reconstruction algorithm possible from 1-byte hop hashes only.

---

## 1. Ground-truth route store (complete)

Source: 1,230,149 multibyte packet observations (30 days) from the ukmesh
TimescaleDB, via the 15432 SSH tunnel.

- **75,625 fully-resolved routes** stored in `pathing-experiment/gold.duckdb`
  — every hop uniquely resolvable (4/6-char prefix → exactly one positioned
  node), known src + observer, consecutive hops ≤150 km, observer terminal hop
  trimmed. 9,175 unique packets · 818 unique nodes · 440 sources · 20 observers.
- Node inventory: 13,244 nodes (10.5k with coords) + 19,118 nodes ever heard
  as rx/src across all 5 months of history + 28,613 distinct path prefixes.
- Split by packet (zero leakage): train 52,452 / val 11,352 / test 11,821 routes.

## 2. Task definition

Each hop's *true* node is known (gold). The algorithm sees only:
the **first byte (2 hex chars)** of each hop's hash, src node, observer node,
and hop count. Candidate pool = every positioned node matching that prefix
(full universe, avg **37.3 candidates/hop**, all 256 prefixes occur).

## 3. Algorithms compared (final holdout test, 11,821 routes)

| algorithm | route acc | hop acc |
|---|---|---|
| random (floor) | 0.2% | 2.8% |
| greedy nearest-chain | 22.6% | 43.7% |
| greedy best-of-both-directions | 25.7% | 49.4% |
| prefix frequency argmax | 67.4% | 88.9% |
| positional frequency argmax | 71.2% | 90.2% |
| Viterbi + freq priors | 96.9% | 99.2% |
| Viterbi + observer shaping | 97.0% | 99.2% |
| **vit_src — Viterbi + freq + corridor interpolation + ITM (WINNER)** | **97.27%** | **99.24%** |
| Viterbi bidirectional | 96.8% | 99.1% |
| Viterbi joint multi-observer | 93.6% | 96.8% |

## 4. Champion design — `vit_src`

Viterbi over the candidate trellis (per-hop candidates from the 1-byte prefix),
with emissions and transitions summed from seven calibrated signals:

**Emissions (per position):**
1. Positional prefix-frequency prior `log(1+count)` ×1.2 — which node usually
   sits at this hop position for this prefix (train-derived)
2. Corridor-level position prior ×1.0 — (src, observer, position) frequency
3. Distance-to-observer shaping (÷80) — relay near the observer
4. **Corridor interpolation** (÷55) — hop i should lie near the point
   `i/(N-1)` of the way from src to observer along the corridor; the single
   most valuable addition for long unseen routes (+9 pp unseen corridors)
5. Multi-observer position anchors ×0.9 (when other observers of the same
   packet heard this position directly)

**Transitions:**
6. Learned hop-transition log-probs ×2.0 (global) + ×1.2 (position-conditional)
7. Distance decay (÷40) with hard 150 km hop cap
8. **ITM radio-viability bonus ×0.8** — pairs the production link model deems
   radio-viable get a boost (+0.44 pp in controlled A/B; +0.06 pp overall test)

Weights tuned by coordinate descent on a val subsample (baseline 97.50% →
97.93% subsample; 97.21% → 97.27% full test).

## 5. Honest breakdown (test, champion)

| ≤3 hops | =4 | 5-6 | 7-8 | 9-12 | **13+** | seen corridor | **unseen corridor** |
|---|---|---|---|---|---|---|---|
| 99.6% | 98.3% | 96.9% | 94.6% | 90.9% | **89.6%** | 97.3% | **93.3%** |

Per-hop accuracy 99.24%. Remaining errors are almost exclusively **single-hop
1-byte prefix collisions** (two real nodes sharing the first byte, e.g.
`9D1A41` vs `9D924A`) — fundamentally unresolvable without the second byte or
more observers. That is the hard floor of the 1-byte task.

## 6. What was tried and rejected (with evidence)

- **LightGBM candidate scorer** (9.5M samples, elevation features): 97.0% vs
  97.4% — the hand-built priors already capture its signal; ML loses on unseen
  corridors (86.4% vs 90.0%). Rejected.
- **Elevation transition penalty** |Δelev|: regressed every variant (raw
  elevation deltas are the wrong model — hilltop→valley drops are normal; data
  only 57% complete). Elevation kept as an ML feature only. Rejected.
- **Bidirectional Viterbi**: neutral-to-negative. Rejected.
- **Joint multi-observer decode** (longest route fixes shared positions):
  93.6% — forced fixes propagate rare errors; observer density too low in this
  dataset (margin-gated version: 91-94%, still below single-observer). Rejected.

## 7. Files

- `01_build_gold.py` — gold route construction (resumable)
- `02_prepare.py` — train/val/test split + 1-byte degradation + candidate maps
- `03_algorithms.py` — all algorithms, tunable `WEIGHTS`, `run_capped.sh` runner
- `04_analysis.py` — per-length / seen-vs-unseen breakdowns
- `06_tune.py` — coordinate-descent weight tuner
- `pathing-experiment/` — gold.duckdb, exp.duckdb, candidates.pkl, ml models
- `ukmesh-multibyte.duckdb` — raw multibyte packets + nodes + heard + prefixes
- `itm_links.csv` — 20,363 ITM-viable pairs (from production link model)

## 8. Production integration notes

1. Port `vit_src` scoring (emissions 1-5, transitions 6-8) into
   `backend/src/path-beta/resolver.ts`; weights are the config surface.
2. The corridor-level prior needs the (src, observer) pair — available in
   packet headers; falls back to prefix-position priors for unseen corridors.
3. ITM bonus: fetch from `node_links` (`itm_viable`), refresh with the link
   worker; bonus-only (never hard-gate — ITM misses real links).
4. Confidence: use the Viterbi per-position margin (already computed) as the
   per-segment confidence for the live-paths colouring.

## 9. Resources (VM crash follow-up)

The 2026-08-03 VM crash coincided with sustained all-8-core LightGBM runs.
Since then every run is capped: `taskset -c 0-3` (4 cores), `nice -n 15`,
single process at a time, and a memory guard that kills the job if free RAM
drops below 2.5 GB. Peak measured during the full test run: ~300 MB / 1 core
per process; VM load stayed < 3. No further crashes.
