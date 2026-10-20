# Pathing

Production packet details, single-observer overlays, multi-observer overlays,
slow finalization and history batches use `path-core/decoder.ts` and the
evidence provider in `path-beta/sharedDecoder.ts`. The previous lazy resolver
is retained only for accuracy comparisons; workers no longer route to it.

The September 2026 changes and comparison against commit `1eefb50` are in
[the accuracy report](pathing-accuracy-2026-09-05.md). The previous 97.27% route /
99.24% hop claim came from a different, narrower experiment and should not be
used as the accuracy of today's traffic.

## Evidence and decoding

The learner builds priors from independently identified multibyte relays.
The live context loads position-prefix, source/receiver corridor, directed
transition and position-specific transition counts. The scoring weights use
counts, not transition probabilities. Source edges have transition position
0; the first relay-to-relay edge has position 1. Observer endpoint edges use
the same directed evidence.

A Viterbi trellis finds a coherent chain of node IDs matching the observed
prefixes. Geography, corridor interpolation, direct receiver anchors and ITM
viability contribute to ranking. Candidate bounds grow with path length so
long flood routes can extend beyond one hop from their observers. Columns
remain capped at 64 candidates. Forward and backward passes share cached
transition scores. Resolution remains in the bounded worker pool; network
context is cached for 15 minutes, and the propagation settling window remains.

Collision-free two- and three-byte observations lock node identities.
Recorded identities survive absent/stale coordinates and contrary terrain
estimates. For inferred routes, repeatedly verified directed training edges
can override a geometric rejection; otherwise physics gates still apply.
A node without coordinates needs observed directed transitions to participate
in an inferred route. Its coordinates stay null so the map breaks the line.

## Observation handling

Flood paths describe recorded relays; direct paths describe routing
instructions and are excluded from observed-route inference and training.
See the [MeshCore packet format](https://github.com/meshcore-dev/MeshCore/blob/main/docs/packet_format.md)
and [firmware path handling](https://github.com/meshcore-dev/MeshCore/blob/main/src/Mesh.cpp).

RX observations retain their last relay even if its short prefix matches the
observer. TX observations remove an appended observer prefix. Legacy rows
without direction retain the previous terminal-trimming convention.
Malformed hashes reject an observation rather than shifting later hops.
Valid one-byte paths support the protocol's 63-hop count instead of stopping
after 24 hops.

An observer can hear several flood copies over different routes. Multi-observer
inference keeps those variants and de-duplicates identical receptions.
Compatible prefixes share a decode; conflicting multibyte routes stay separate.
A coarse observation compatible with several confirmed routes keeps its own
group. Each projected path ends at its own observer, without adding edges to
another observer's branch.
Map labels use each branch's own relay identities; coordinates alone cannot
identify co-located nodes.

A held path is reconsidered across the entire chain when evidence changes.
A better chain must clear the existing score margin; holding an assignment
does not turn its confidence into 100%.

The learner preserves original positions across missing or ambiguous relays
and cannot learn a shortcut across a missing relay. Its version is now
`path-learning-v5-contiguous`; the next normal rebuild republishes corrected
priors without a schema migration.

## Ownership and scope

- `path-beta/resolver.ts`: scoped evidence loading, observation preparation,
  cached context and API projections.
- `path-beta/championPriors.ts`: count aggregation and bounded prior loading.
- `path-core/decoder.ts`: candidate indexing, scoring, Viterbi traversal and
  ambiguity margins.
- `path-learning/rebuild.ts`: multibyte training and atomic publication.
- `pathing/pathingPublicDto.ts`: explicit public DTO projection. Add new
  resolver fields here before clients can receive them.

UKMesh retains historical `northeast` and `teesside` evidence through the
shared network scope. Private-node exclusions and visibility-generation cache
fences continue to apply. Test networks remain isolated.

## Reproducing accuracy checks

Run from `backend/`. Capture uses a repeatable-read, read-only transaction.
Replays are offline and do not update priors, online state or production
caches. Local snapshots and traces belong under `artifacts/path-accuracy/`,
which is ignored by Git.

```sh
node --env-file=../.env --import tsx src/tools/pathAccuracy.ts capture /tmp/path-snapshot.json
npm run path:accuracy -- baseline /tmp/path-baseline 1eefb50
npm run path:accuracy -- replay /tmp/path-snapshot.json /tmp/new.json current test
npm run path:accuracy -- replay /tmp/path-snapshot.json /tmp/old.json /tmp/path-baseline/backend/src/path-lazy/lazyResolver.ts test
npm run path:accuracy -- compare /tmp/old.json /tmp/new.json /tmp/comparison.json
```

Use `PATH_ACCURACY_MODE=multi` for multi-observer replay. Substitute
`path-beta/sharedDecoder.ts` in the baseline path to compare the old beta
resolver. `PATH_ACCURACY_DAYS` selects the capture window (default 14, maximum
45); `PATH_ACCURACY_NETWORK` defaults to `ukmesh`.

The harness reserves PostgreSQL packet-hash buckets 70–84 for development and
85–99 for evaluation. **Verify that the running learner uses the default 70%
training split before capture**; training on more than 70% would contaminate
these held-out packets. Node/link metadata is a current snapshot, so this is
a packet-held-out comparison, not a historical simulation of metadata at the
time of each reception.

Truth comes only from three-byte flood paths with uniquely matching public
node identities and a positioned observer. One-byte replay hides the extra
bytes in every sibling observation. Cases with unresolved or colliding truth
are reported as excluded; geographic distance does not exclude a route.
Comparisons require identical snapshots and labels, count unresolved guesses
as errors, report regressions and corrections, and bootstrap unique relay
chains rather than treating recurrent copies as independent routes.
