# Stage 3 pathing accuracy summary

Date: 2026-08-03

Branch: `pathing/stage3`

Base: `0194f6d` (`pathing/merge-base`, merged shared decoder + beta port + harness)

## Outcome

The production lazy Viterbi now receives the one-byte, endpoint, geographic,
transition, and radio evidence used by the offline champion. On the full
1,259-packet gate it improved from **11.5% route / 15.3% hop** to **88.0% route
/ 97.4% hop**, with no unresolved positions and no resolver failures.

The requested prior-supported target passed: **96.2% route / 99.3% hop** versus
the target of at least 90% route. The unseen-corridor result was **83.3% route /
95.7% hop**: a 69.1-point route improvement over the 14.2% baseline, but 1.7
points short of the requested 85% route target.

## Diagnosis, measured rather than inferred

### Candidate construction

The positioned-node universe contains an average of 36.52 candidates per
one-byte prefix (median 37, p90 45, maximum 55). Of the 256 prefixes, 249 have
more than the old `MAX_COL=24`. Candidate rows were ordered by `node_id`, so the
old cap discarded evidence arbitrarily.

In the aligned 200-packet diagnostic, raising the cap to 128 improved route/hop
from 52.3%/70.7% to 55.2%/72.4%. The final cap is 64, which admits the measured
full universe (maximum 55) while retaining a bounded trellis. If a future
universe exceeds 64, candidates are scored by their emissions before truncation.

The aggregate evidence guards were a separate failure mode for packet hashes
with many incompatible historical groups. Four of 200 routes hit
`PATH_HISTORY_LIMIT`. Raising the bounded unique-hash and candidate-node loads
to 256 and 20,000, and deduplicating `(receiver, hop_count)` anchor observations,
removed all failures on the full 1,259-packet run.

### Observer bounds

The old bounds were constructed only from receivers. Source and receiver
endpoints are now included and the box is padded by `MAX_HOP_KM` (150 km), as in
the experiment. Disabling the padded bound did not help: the controlled run
fell from 55.2%/72.4% to 54.1%/71.9%. The bound is therefore retained as a useful
geographic filter, not removed.

### NULL abstention

The original 200-packet run left 87.6% of hop positions unresolved. Making
`NULL_BASELINE` noncompetitive by itself reduced unresolved output by 3.3
points but improved route accuracy only from 51.7% to 52.3%; abstention was real,
but it was not the main accuracy cause. The decoder now always chooses the best
real candidate when a column is nonempty and uses NULL only as a structural
fallback for an empty column. Marginal gaps still set the `ambiguous` flag.

### Prior granularity

`path_prefix_priors` contained zero two-character rows. It had 8,846 four-char
rows and 7,082 six-char rows, so the one-byte-degraded resolver missed the table
entirely. A one-byte fallback in the controlled diagnostic raised route/hop to
56.4%/73.0%, including 85.4%/93.2% on supported routes.

The replacement tables store true two-character keys. The decoder uses a
global one-byte frequency backoff (`log1p(count) × 0.3`) and the champion's
position-specific frequency (`log1p(count) × 1.2`). The global backoff was kept
because its 300-packet A/B improved unseen-corridor route accuracy; larger
weights regressed.

### Harness/pipeline mismatches

Two harness defects hid how much the decoder could recover:

- Gold rows retained historical `northeast`/`teesside` labels while positioned
  nodes had been relabelled `ukmesh`, producing empty candidate columns. The
  harness now resolves through the requested compatibility scope.
- Gold packets span 45 days while lazy resolution defaulted to the latest 168
  hours. Historical evaluation now passes the selected packet timestamp and
  reads a bounded ±30-second observation window.

The gold builder now matches the experiment's eligibility rules: known and
positioned source/receiver, uniquely resolved multibyte relays, terminal
receiver hash trimmed, and every source→relay→receiver edge at most 150 km.
Corridor novelty now means an exact source/receiver pair exists in the sampled
training prior rather than merely occurring earlier in raw packet history.

## Implemented evidence model

The shared decoder now scores the following champion signals:

- global one-byte prefix frequency: `log1p(count) × 0.3`;
- positional one-byte prefix frequency: `log1p(count) × 1.2`;
- exact `(source, receiver, position)` corridor frequency: `log1p(count) × 1.0`;
- general candidate-to-observer distance: `-distance / 80 km`;
- direct multi-observer position anchor: `-0.9 × distance / 80 km`, with a
  150 km hard anchor gate;
- source→receiver corridor interpolation at `i/(N-1)`: `-distance / 55 km`;
- global directed transition: `log1p(count) × 2.0`;
- position-conditional transition: `log1p(count) × 1.2`, including source and
  receiver endpoint edges;
- hop geography: `-distance / 40 km`, with a hard 150 km cap;
- `node_links.itm_viable` or `force_viable`: bonus `+0.8`, never a hard gate.

ML, edge-score, motif, and generic observed-link weights are zero because the
champion did not use them. The new priors and bonuses are enabled by default and
retain environment switches/weight overrides for controlled ablation.

## Rebuild and migration

Migration `033_path_champion_priors.sql` adds:

- `path_position_prefix_priors`;
- `path_corridor_priors`;
- `path_position_transition_priors`.

They are replaced transactionally with the existing prefix, transition, edge,
motif, and calibration rows under the same publication lease.

The prior rebuild now mirrors the experiment more closely: a deterministic 70%
packet-hash training split over 30 days, with identical packet/observer/source/
route observations deduplicated before aggregation. This avoids the old 6.25%
(`hash % 16 == 0`) sample wasting its budget on repeated copies. UKMesh packet
training uses the same `ukmesh`/`northeast`/`teesside` compatibility scope as the
resolver while nodes and links stay in the current UKMesh inventory.

The verified rebuild published, for the UKMesh model:

```text
packets=403302 top1=0.407 scale=0.778
position-prefix rows=3837 corridor rows=21450
position-transition rows=6290 edge rows=57991 motif rows=123281
```

The migration and rebuild were run locally for verification. No service was
deployed, pushed, or restarted.

## Full before/after gate

Each value is route accuracy / hop accuracy. Baseline values are from
`BASELINE-PATHING.md`; after values are the final `n=1259` run. The corrected
gold eligibility and sampled-prior corridor definition change stratum sizes, so
the table is an operational before/after rather than a paired-packet experiment.

| stratification | baseline lazy | final n | final lazy |
|---|---:|---:|---:|
| overall | 11.5% / 15.3% | 1,259 | **88.0% / 97.4%** |
| prior rare (≤1) | 6.4% / 10.2% | 832 | **83.8% / 96.4%** |
| prior mid (2–7) | 11.8% / 26.1% | 10 | **100.0% / 100.0%** |
| prior supported (≥8) | 26.7% / 33.2% | 417 | **96.2% / 99.3%** |
| route ≤3 | 10.4% / 11.1% | 390 | **93.1% / 96.5%** |
| route 4 | 11.9% / 14.9% | 145 | **81.4% / 94.8%** |
| route 5–6 | 12.2% / 13.9% | 262 | **90.1% / 98.0%** |
| route 7–8 | 13.7% / 17.1% | 201 | **83.1% / 97.0%** |
| route 9–12 | 9.9% / 12.4% | 216 | **88.9% / 98.3%** |
| route 13+ | 13.7% / 21.8% | 45 | **71.1% / 97.0%** |
| corridor seen | 7.0% / 6.9% | 512 | **94.9% / 99.2%** |
| corridor unseen | 14.2% / 19.7% | 747 | **83.3% / 95.7%** |

All four jobs on the final corpus:

| job | route | hop | unresolved | failures |
|---|---:|---:|---:|---:|
| legacy-greedy | 33.4% | 68.7% | 19.8% | 0 |
| **lazy-viterbi** | **88.0%** | **97.4%** | **0.0%** | **0** |
| beta-single | 27.5% | 64.6% | 15.8% | 0 |
| beta-multi | 2.7% | 7.6% | 90.0% | 0 |

## Verification

Commands completed successfully:

```text
cd backend
npm install
npm run build
npm test                         # 249 passed, 0 failed
npx tsx src/path-lazy/evaluate.ts ukmesh 1259
```

The full harness was run as one `taskset -c 0-3 nice -n 15` process. Rebuilds
and harnesses were never run concurrently.

## Remaining gap

The port closes most of the production/offline gap but does not reproduce the
offline 97.27% route score. The largest residual is route-level sensitivity to
one wrong byte-prefix collision: hop accuracy is 97.4%, yet complete-route
accuracy is 88.0%. This compounds most strongly on 13+ routes (71.1% route with
97.0% hop). Unseen corridors remain 1.7 points below the requested target.

Likely contributors are differences between the live compatibility-scoped
rebuild and the frozen experiment corpus, capped top-N prior tables, and genuine
one-byte collisions with equally plausible learned spines. The full test set
was not used for another tuning pass. Further work should use a frozen
train/validation/test export and inspect the remaining wrong-hop margins before
changing weights; beta job accuracy remains a separate later-stage issue and
was intentionally not changed under this task's file ownership.
