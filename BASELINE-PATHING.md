# Pathing accuracy baseline

Phase 0 baseline for `ukmesh`, captured 2026-08-03 from the local TimescaleDB
snapshot. The harness selects multibyte packets from the last 45 days, keeps
only packets whose full relay hashes uniquely identify every relay, degrades
the resolver-facing packet rows to one-byte prefixes, and scores the four
path-resolution jobs:

- legacy-greedy — `lazyResolvePathLegacy`
- lazy-viterbi — `lazyResolvePath`
- beta-single — `resolveBetaPathForPacketHash`
- beta-multi — `resolveMultiObserverBetaPath`

Each cell is `route accuracy / hop accuracy`. Route accuracy means the complete
relay chain is correct; hop accuracy is correct relay positions divided by all
gold relay positions. `n` is the number of gold packets in the stratum.

The primary run requested 1,500 packets and yielded 1,259 valid gold packets:
309 routes of length ≤3, 126 of length 4, 196 of length 5–6, 190 of length
7–8, 314 of length 9–12, and 124 of length 13+. Corridor novelty is source →
receiver: **seen** means the pair occurred earlier in the preceding 120-day
prior-building window; missing source IDs are classified as unseen.

## Primary run: target 1,500, valid gold n=1,259

| stratification | n | legacy-greedy | lazy-viterbi | beta-single | beta-multi |
|---|---:|---:|---:|---:|---:|
| overall | 1,259 | 20.7% / 44.6% | 11.5% / 15.3% | 17.0% / 20.0% | 2.1% / 1.9% |
| prior rare (≤1) | 931 | 16.0% / 39.7% | 6.4% / 10.2% | 17.4% / 17.8% | 1.9% / 1.8% |
| prior mid (2–7) | 17 | 11.8% / 44.6% | 11.8% / 26.1% | 58.8% / 52.2% | 0.0% / 0.0% |
| prior supported (≥8) | 311 | 35.4% / 62.0% | 26.7% / 33.2% | 13.5% / 26.2% | 2.9% / 2.3% |
| route ≤3 | 309 | 35.9% / 49.5% | 10.4% / 11.1% | 53.7% / 62.8% | 6.8% / 8.0% |
| route 4 | 126 | 37.3% / 54.8% | 11.9% / 14.9% | 23.0% / 43.1% | 1.6% / 4.8% |
| route 5–6 | 196 | 26.5% / 52.7% | 12.2% / 13.9% | 8.2% / 34.6% | 2.0% / 3.5% |
| route 7–8 | 190 | 17.4% / 53.7% | 13.7% / 17.1% | 1.1% / 17.5% | 0.0% / 1.7% |
| route 9–12 | 314 | 5.4% / 41.3% | 9.9% / 12.4% | 0.3% / 10.4% | 0.0% / 0.7% |
| route 13+ | 124 | 0.8% / 33.5% | 13.7% / 21.8% | 0.0% / 6.3% | 0.0% / 0.0% |
| corridor seen | 470 | 21.7% / 42.0% | 7.0% / 6.9% | 19.8% / 20.9% | 2.3% / 1.8% |
| corridor unseen | 789 | 20.2% / 45.9% | 14.2% / 19.7% | 15.3% / 19.5% | 2.0% / 2.0% |

The primary run had one lazy-viterbi `PATH_HISTORY_LIMIT` failure. It is
included in that job's denominator and scored as unresolved; legacy-greedy,
beta-single, and beta-multi had zero resolver failures.

## Smoke run: target 200, valid gold n=173

| stratification | n | legacy-greedy | lazy-viterbi | beta-single | beta-multi |
|---|---:|---:|---:|---:|---:|
| overall | 173 | 20.2% / 48.2% | 10.4% / 14.0% | 18.5% / 21.6% | 0.0% / 0.9% |
| prior rare (≤1) | 131 | 18.3% / 44.0% | 5.3% / 9.3% | 18.3% / 19.3% | 0.0% / 1.1% |
| prior mid (2–7) | 4 | 25.0% / 76.2% | 25.0% / 52.4% | 75.0% / 76.2% | 0.0% / 0.0% |
| prior supported (≥8) | 38 | 26.3% / 61.7% | 26.3% / 28.7% | 13.2% / 25.4% | 0.0% / 0.0% |
| route ≤3 | 45 | 28.9% / 48.2% | 4.4% / 5.5% | 51.1% / 63.6% | 0.0% / 3.6% |
| route 4 | 20 | 35.0% / 58.8% | 10.0% / 13.8% | 40.0% / 55.0% | 0.0% / 7.5% |
| route 5–6 | 24 | 33.3% / 52.7% | 16.7% / 17.6% | 4.2% / 16.0% | 0.0% / 0.0% |
| route 7–8 | 26 | 19.2% / 51.8% | 15.4% / 15.4% | 0.0% / 10.8% | 0.0% / 0.0% |
| route 9–12 | 41 | 2.4% / 43.0% | 9.8% / 12.5% | 0.0% / 16.5% | 0.0% / 0.0% |
| route 13+ | 17 | 5.9% / 48.2% | 11.8% / 17.6% | 0.0% / 11.8% | 0.0% / 0.0% |
| corridor seen | 60 | 20.0% / 50.5% | 1.7% / 1.6% | 23.3% / 25.7% | 0.0% / 1.3% |
| corridor unseen | 113 | 20.4% / 47.1% | 15.0% / 20.0% | 15.9% / 19.6% | 0.0% / 0.6% |

## Execution and safety notes

The compatible CLI was used:

```text
npx tsx src/path-lazy/evaluate.ts ukmesh 200
npx tsx src/path-lazy/evaluate.ts ukmesh 1500
```

The harness uses SELECTs only. Beta's `touchPredictedOnline` option is false,
and its direct database packet reads are degraded in memory for evaluation;
the database rows are not modified. Beta coordinate DTOs are mapped back to
the unique node coordinate index loaded for the gold set.
