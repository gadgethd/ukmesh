# Message-Spam Detection (Spam Watch)

Detects people spamming the MeshCore mesh by finding **repeated, near-duplicate
messages** sent in short time windows, groups them into incidents, and estimates
a **coarse** origin area. Powers the public `ukmesh.com/spam` dashboard.

This is distinct from the older advert/identity-spoof detector
(`backend/src/mqtt/spamDetector.ts`), which scores repeater/device adverts. This
system is about **chat-message** abuse.

## How it works

```
packets (type 5, decoded channel msgs)
        │  loadRecentMessages()       backend/src/spam/repository.ts
        ▼
MessageRecord[]  (one per packet_hash, with observers)
        │  clusterMessages()          backend/src/spam/cluster.ts
        ▼
Incident[]       (near-duplicate text, close in time)
        │  estimateOrigin()           backend/src/spam/origin.ts
        │  sanitizeIncident()         backend/src/spam/sanitize.ts
        ▼
spam_message_incidents / spam_message_members   (public_json is pre-sanitized)
        │  getPublic*()               backend/src/spam/repository.ts
        ▼
/api/spam/messages/*  →  Spam Watch dashboard (frontend SpamPage)
```

Only decoded channel messages are used (MeshCore public/known-key channels where
`payload.decrypted.message` exists). Direct messages and unknown-key channels stay
encrypted and are never inspected.

### Pipeline stages

1. **Normalize** (`normalize.ts`) — lowercase, trim, collapse whitespace,
   normalize punctuation, collapse stretched repeats (`heyyyy`→`heyy`), and
   strip/canonicalize URLs (drop scheme/`www.`/query so tracking variants
   collapse). `ukmesh.com/spam` is recognised as a marker — supporting evidence,
   never sufficient on its own.
2. **Fuzzy match** (`similarity.ts`) — message similarity blends trigram Dice,
   token-set Jaccard and Levenshtein ratio; a shared canonical URL is a strong
   signal. Username similarity adds prefix/substring affinity so `John`,
   `John2`, `John_UK` read as related.
3. **Cluster** (`cluster.ts`) — time-ordered greedy clustering inside a join
   window. Similar usernames lower the text-similarity bar but never create a
   cluster alone. A qualification gate then separates abuse from normal chatter
   (see below).
4. **Estimate origin** — two methods, path-based preferred:

   **a. Relay-path adjacency chain-walk** (`spamResolver.ts`, used first for
   public incidents). Every reception carries its ordered relay path; the **first
   repeater is the source of truth** (the spammer transmitted into it). We take
   the **closest receivers** (lowest hop count) and **walk each path backward from
   the observer**, hop by hop: at each step we pick the candidate node that has a
   confirmed RF link (`node_links`) to the previously-resolved node, else the
   nearest candidate within range. Because a consecutive `(AA,BB)` hash pair is
   only realisable where two such repeaters sit within range, the adjacency chain
   disambiguates the otherwise-ambiguous 1-byte hashes — and the **random
   deviations in the middle of the paths** give many independent chains that
   converge on the same source. The chain's first repeaters are then combined into
   a geographic consensus. Confidence is **tempered by the closest observer's hop
   count**: a 2-hop reception is trusted (short chain); a source only heard 5–7
   hops out is honestly less certain (long chain, compounding per-hop error) even
   when the chains agree. (`spamResolver.ts` is an independent copy of the lazy
   path resolver so the production resolver is never touched.)

   **b. Observer-signal fallback** (`origin.ts`) — when paths can't be resolved
   (no path hashes, or no confident consensus), fall back to the observer
   estimate below. A flood radiates outward from its source,
   so an observer that heard it at a **low hop count** sits close to the source
   while distant relays only heard it after it crossed the network. The estimate
   is therefore **anchored on the closest-receiver cohort** (the lowest hop
   counts available — `min-hop + originNearHopSlack`, at least everyone within
   `originNearHopMax` hops); their location gives the centroid, and the radius is
   their spread plus a per-hop allowance. Confidence comes from how close the
   nearest reception was — both absolutely and *relative* to how far the flood
   otherwise travelled (a 2-hop reception of something that otherwise relayed 14
   hops is strong localisation). This works UK-wide: dense areas yield tight,
   high-confidence pins; sparse areas (where the closest reception is several
   hops out) honestly produce a broad radius and lower confidence rather than a
   false-precise point. Snaps to a coarse named UK region.

   **Coverage note:** localisation quality tracks observer density — strong
   across England, broader/lower-confidence in the far north of Scotland and the
   islands. With fewer than `originMinObservers` geolocated receivers the origin
   is reported as `insufficient` (no map zone) rather than guessed.
5. **Sanitize** (`sanitize.ts`) — the single gate to public output (see Privacy).
6. **Persist** (`repository.ts`) — upsert incidents; `first_seen` preserved via
   `LEAST`; in-window incidents that no longer cluster are removed; older history
   is frozen.

## What counts as spam (and what doesn't)

A cluster only becomes an incident when it is genuine abuse, not normal traffic:

- **Requires** `minTransmissions` near-duplicates with `minBurst` inside one
  burst window.
- **Requires substantive content** — at least `minContentChars`, *or* a URL,
  *or* the spam marker. Trivial/connectivity messages (`test`, `ack`, `morning`,
  `gm`, …) never qualify on their own.
- **Requires a suspicious sender pattern** — a single repeating sender, one
  dominant sender, or **rotating similar usernames**. Many *dissimilar* people
  sending the same thing is treated as broad legitimate chatter, not one abuser.
- The **test channel is excluded by default** (it carries repeated test traffic
  by design). Configurable via `SPAM_MESSAGE_EXCLUDE_CHANNELS`. A flood that
  *also* hits another channel (e.g. Public) is still caught via those copies.
- Content is dampened only when it is **dominated by** connectivity/test tokens
  — i.e. once benign words, bare numbers and 1–2-char fragments are removed,
  nothing substantive is left (`test`, `rx ack 73`, `test 123`). A templated
  message that merely *contains* the word `test` (e.g. an automated
  `ANDY KIRBY MESHCORE(TM) 00:16:4: !test` flood) keeps real words and is **not**
  treated as testing — so this camouflage no longer hides spam.
- A **sustained high-volume flood** (≥ `floodMinMessages` near-duplicates) is a
  strong automated-abuse signal on its own and is scored up — but a benign
  test/connectivity flood is never rewarded, so it stays below the public floor
  no matter how large it grows.

## Incident lifecycle

- **active / ongoing** while the last matching message is within
  `SPAM_MESSAGE_ONGOING_WINDOW_MIN`.
- **closed** after that cooldown elapses with no new matching messages.

The periodic analyzer recomputes recent incidents every
`SPAM_MESSAGE_INTERVAL_MS` and re-evaluates active→closed transitions each run.

## Configuration

All knobs live in `backend/src/spam/config.ts` and read from the environment.

| Env var | Default | Meaning |
| --- | --- | --- |
| `SPAM_MESSAGE_ANALYZER_ENABLED` | `true` | Run the in-process periodic analyzer |
| `SPAM_MESSAGE_INTERVAL_MS` | `300000` | Analyzer interval (5 min) |
| `SPAM_MESSAGE_WINDOW_HOURS` | `24` | Rolling window the analyzer (re)clusters |
| `SPAM_MESSAGE_TEXT_SIM` | `0.82` | Min text similarity to join a cluster |
| `SPAM_MESSAGE_TEXT_SIM_NAME` | `0.6` | Lower text bar when usernames are similar |
| `SPAM_MESSAGE_NAME_SIM` | `0.7` | Min username similarity (evidence) |
| `SPAM_MESSAGE_JOIN_WINDOW_MIN` | `30` | Max gap to join the same incident |
| `SPAM_MESSAGE_MIN_TRANSMISSIONS` | `8` | Min distinct messages for an incident |
| `SPAM_MESSAGE_BURST_WINDOW_MIN` | `10` | Burst window |
| `SPAM_MESSAGE_MIN_BURST` | `3` | Min messages inside one burst window |
| `SPAM_MESSAGE_MIN_CONTENT_CHARS` | `12` | Min canonical length (unless URL/marker) |
| `SPAM_MESSAGE_MAX_INDEPENDENT_SENDERS` | `4` | Above this (dissimilar) → broad chatter |
| `SPAM_MESSAGE_DOMINANT_SENDER_SHARE` | `0.6` | One sender at this share qualifies |
| `SPAM_MESSAGE_FLOOD_MIN_MESSAGES` | `25` | ≥ this many near-duplicates → sustained-flood boost |
| `SPAM_MESSAGE_EXCLUDE_CHANNELS` | `test` | Channel labels skipped (comma list) |
| `SPAM_MESSAGE_PUBLIC_MIN_SCORE` | `0.5` | Confidence floor for public list / status |
| `SPAM_MESSAGE_ONGOING_WINDOW_MIN` | `30` | How long an incident stays "ongoing" |
| `SPAM_MESSAGE_ORIGIN_USE_PATHS` | `true` | Anchor origin on the relay-path first repeater |
| `SPAM_MESSAGE_ORIGIN_PATH_MAX_PACKETS` | `40` | Max closest-receiver transmissions resolved |
| `SPAM_MESSAGE_ORIGIN_PATH_MIN_VOTES` | `3` | Min resolved paths before a path anchor is trusted |
| `SPAM_MESSAGE_ORIGIN_PATH_CLUSTER_KM` | `45` | Consensus cluster radius (outliers beyond it dropped) |
| `SPAM_MESSAGE_ORIGIN_PATH_AMBIGUOUS_WEIGHT` | `0.4` | Weight of an ambiguous vs certain repeater resolution |
| `SPAM_MESSAGE_ORIGIN_MIN_OBSERVERS` | `2` | Min observers before estimating origin (fallback) |
| `SPAM_MESSAGE_ORIGIN_NEAR_HOP_MAX` | `2` | Receivers within this many hops count as "near source" |
| `SPAM_MESSAGE_ORIGIN_NEAR_HOP_SLACK` | `1` | Width (hops) of the closest-receiver cohort that anchors it |
| `SPAM_MESSAGE_ORIGIN_PER_HOP_KM` | `12` | Radius allowance per hop (≈ LoRa link range) |
| `SPAM_MESSAGE_ORIGIN_MIN_RADIUS_KM` | `8` | Floor on reported radius (privacy) |
| `SPAM_MESSAGE_COARSEN_STEP_DEG` | `0.1` | Public coordinate coarsening (~11 km) |
| `SPAM_MESSAGE_REGION_SNAP_KM` | `70` | Max distance to snap to a named region |

**Tuning sensitivity:** lower `SPAM_MESSAGE_PUBLIC_MIN_SCORE` to surface more
(noisier) clusters; raise `SPAM_MESSAGE_MIN_TRANSMISSIONS` / `MIN_CONTENT_CHARS`
to be stricter.

## Confidence scores

Two separate confidences are shown:

- **Detection confidence** (incident `confidence`, 0–1) — how strongly the
  pattern resembles spam. Built from volume, burst density, sustained-flood
  size, repeated links, the spam marker, and rotating sender names; content
  dominated by connectivity/test tokens is dampened. `High ≥ 0.66`,
  `Medium ≥ 0.4`, else `Low`. Only incidents at/above
  `SPAM_MESSAGE_PUBLIC_MIN_SCORE` appear publicly by default.
- **Origin confidence** (origin `confidence` + `level`) — how trustworthy the
  reported *zone* is (region + radius), driven mainly by how close the nearest
  reception was (low hop count) and how much it stands out from the flood's
  overall reach, plus corroboration from other near-source receivers.
  `insufficient` means too few geolocated observers to triangulate; the map
  zone is then omitted. Note that confidence is confidence in the *zone* — a
  larger radius absorbs the uncertainty, so "high confidence" means the source
  is very likely inside that (coarse) area, not that it is pinpointed.

A single low-hop receiver gives a confident *region* but a coarse radius; more
near-source receivers tighten the radius and raise confidence further. On the
dashboard, click **Estimated origin** (or "View area on map") to see the coarse
confidence circle on a map.

## Privacy model

Abuse mitigation only. The public API serves the pre-computed `public_json`
column, which is produced solely by `sanitizeIncident()`:

- **Sender names** are redacted to a short non-identifying hint (`John_UK`→`Jo…`)
  that still conveys variant similarity. Raw names stay in local-only columns.
- **Message samples** strip URLs (`[link]`, marker → `[spam-link]`), `@mentions`
  (`[mention]`) and long hex/number runs (`[id]`), and are length-bounded.
- **Locations** are coarse only: a region label, a confidence level, and an
  optional broad heat zone (centre coarsened to a ~0.1° grid, radius floored at
  `ORIGIN_MIN_RADIUS_KM` and bucketed to 5 km). Exact coordinates, observer ids
  and node metadata are never published.
- Clusters are labelled **"suspected spam cluster"** — neutral, not an
  accusation of any named individual.
- No IPs, credentials, public keys or channel keys are exposed.

## API

| Endpoint | Description |
| --- | --- |
| `GET /api/spam/messages/status` | Current status: ongoing?, counts, last activity |
| `GET /api/spam/messages/incidents?status=&minConfidence=&limit=&offset=` | Sanitized incident list |
| `GET /api/spam/messages/incidents/:id` | One incident + sanitized timeline |

`minConfidence` defaults to `SPAM_MESSAGE_PUBLIC_MIN_SCORE`; pass `0` to include
lower-confidence clusters (the dashboard's "show lower-confidence" toggle).

## Deployment

The derived tables are created by migration
`backend/src/db/migrations/005_spam_message_incidents.sql` (additive, no
destructive changes). On deployments with `DATABASE_SKIP_SCHEMA_INIT=true`
(production), apply it manually like prior migrations:

```bash
docker compose exec -T timescaledb psql -U meshcore -d meshcore -v ON_ERROR_STOP=1 \
  < backend/src/db/migrations/005_spam_message_incidents.sql
docker compose exec -T timescaledb psql -U meshcore -d meshcore \
  -c "INSERT INTO schema_migrations (name) VALUES ('005_spam_message_incidents.sql') ON CONFLICT DO NOTHING;"
```

Then rebuild and restart:

```bash
docker compose build backend app-ukmesh website-ukmesh
docker compose --profile tunnel up -d backend app-ukmesh website-ukmesh
```

The in-process analyzer starts automatically. To backfill historical incidents
or run a one-off pass:

```bash
# dry run over 30 days (no writes)
docker compose exec backend node dist/tools/recomputeSpamMessages.js --hours 720
# apply
docker compose exec backend node dist/tools/recomputeSpamMessages.js --hours 720 --apply
```

## Tests

Pure logic (normalization, fuzzy matching, username similarity, clustering,
ongoing/closed status, origin scoring, sanitization) is covered by unit tests:

```bash
cd backend && npm test
```
