# Privacy & data retention

Applies to ukmesh.com / app.ukmesh.com (MeshCore Analytics). UK GDPR + DPA 2018.
Contact: ukmesh@proton.me

## 1. Error diagnostics telemetry

**What is stored** (per event): kind (error/warning/unhandledrejection/crash),
truncated message (≤500 chars) and stack (≤2000 chars stored; browser input is
capped at 4000), page pathname supplied by the client (no query string from the
shipped client), userAgent (≤500 chars), HMAC-hashed source IP and bounded
occurrence/fingerprint fields. The stored record has no raw IP, account ID or
cookie contents. This does not guarantee that free-text error messages contain
no personal information.

**Lawful basis — legitimate interest (Art 6(1)(f)).** Three-part test:

- **Purpose test:** operating a reliable public service requires knowing when
  client-side failures occur; this telemetry has already root-caused real
  production crashes (deck.gl/maplibre, worker-file MIME regressions).
- **Necessity test:** routine diagnostics do not require an account identifier.
  The event store uses a pseudonymous source hash and bounded, truncated fields.
- **Balancing test:** diagnostic records use a pseudonymous source hash, have
  a 30-day retention target, are rate-limited (10/min/IP) and sampled
  (20/hr/source), and use the self-hosted event store. Individual
  impact is minimal; service reliability benefit is direct.

**Retention policy:** 30 days. Enforcement requires the operational retention
loop (`DATA_LIFECYCLE_RETENTION_ENABLED=true` and the target allowlist); verify
actual scheduling and expiry with the [lifecycle inventory](db-lifecycle.md).
The client supports `?telemetry=off`. Routine expiry removes old diagnostics;
requests concerning potentially identifying error text go to the contact above.

Public Web Vitals use a separate endpoint. It accepts only LCP/CLS/INP, a bounded
number and one of six fixed public routes. The client omits credentials and
identifiers, reports at most once per metric per document and supports the same
opt-out. Reports update aggregate process metrics and create no database rows;
see [the operations contract](operations.md#metrics).

## 2. Mesh packet database

The service stores radio traffic observed on the MeshCore network. The table
records policy targets; actual enforcement depends on enabled jobs and the
reviewed lifecycle allowlist, verified through the inventory above.

| Data | Retained | Rationale |
|---|---|---|
| `packets` — message content + raw bytes + metadata | **30-day policy target** (enabled Timescale chunk policy + bounded exact-row boundary cleanup) | network diagnostics; content is not kept long-term |
| `packet_decryptions` — full decrypted messages | **30 days** (operational retention loop) | diagnostics only |
| `owner_packet_share_deliveries` — validated packet payload and delivery metadata | **30 days after terminal delivery** (operational retention loop) | durable site-to-site delivery and deduplication; retryable rows remain until terminal |
| `packet_paths` — path metadata (node IDs, timing, signal, hashes) | **indefinite, content-stripped** | raw pathing analytics is the product |
| `multibyte_path_facts`, `multihop_paths_weekly`, `path_*_priors` — derived path stats | indefinite | derived, pseudonymous |
| `packet_daily_stats` / `packet_hourly_stats` | indefinite | anonymous aggregates |

**Lawful basis — legitimate interest (Art 6(1)(f)).**

- **Purpose test:** operating and improving the network (pathing, coverage,
  diagnostics) requires the packet data; the owner portal requires status data.
- **Necessity test:** the policy limits content to 30 days; long-term packet
  retention is limited to path metadata with no message content and no coordinates, which
  is the minimum needed for pathing analytics.
- **Balancing test:** pseudonymous node identifiers only (linkable to an
  individual through owner registration or other identifying context); no content in
  long-lived tables; no individual profiling; disclosed in the privacy notice.

**Design rules (binding):**

- Retained packet-path history never gains `payload`, `raw_hex`, or coordinate
  columns. Current node/location state is a separate model with public/owner DTO
  rules. Content-bearing tables always have an explicit retention policy.
- Retained path visibility is derived from the current private-node prefix
  tables. Stored `packet_paths` flags are a trigger-maintained cache and are
  never the authority for a public or derived read.
- Decryption outputs land only in short-retention tables (30 days).
- New derived tables get a lifecycle policy at creation.
- Erasure requests require identifying the relevant node-linked records and a
  reviewed maintenance procedure covering packets, retained paths, decryptions
  and derived facts. This repository does not provide a general erasure CLI.
  Hiding a node or changing a privacy prefix is not physical deletion; removing
  privacy policy before retained evidence is handled can expose old data.

## 3. Device status telemetry

Status samples retain a JSON telemetry object for the authenticated owner
portal. The unauthenticated `/node-status/latest` and `/mqtt-nodes` APIs expose
only their explicit public fields: node identity, sample/availability data,
battery, uptime, airtime, channel utilization, and recent packet counts. They
do not return the telemetry object or forward unrecognized fields. Wi-Fi
identifiers, MQTT broker/account details, reset and configuration fields,
unrecognized firmware/status blobs and filesystem details stay behind owner
authentication. The public map separately permits its explicit `firmware_version`
and `hardware_model` node fields; that does not expose the original status
envelope. Unrecognized keys stay in storage until they
are deliberately added to a response contract.

Existing status samples do not need a database rewrite: public responses are
projected through the same field allowlist regardless of when the sample was
stored.

## 4. Cookies

Preferences such as consent, theme and saved local views use browser storage.
Owner login also uses the encrypted `meshcore_owner_session` cookie (HttpOnly,
SameSite=Strict, Secure on HTTPS, seven-day expiry), backed by an active
server-side session. `meshcore_owner_csrf` is a readable same-site cookie for
mutation protection. The local operator site has its own session/CSRF cookies.
These are authentication/security state, not advertising identifiers.

Browser storage must be described alongside cookies; using localStorage does
not by itself remove cookie/storage obligations. See the [ICO guidance on
cookies and similar technologies](https://ico.org.uk/for-organisations/direct-marketing-and-privacy-and-electronic-communications/guide-to-pecr/cookies-and-similar-technologies/).
