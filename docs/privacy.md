# Privacy & data retention

Applies to ukmesh.com / app.ukmesh.com (MeshCore Analytics). UK GDPR + DPA 2018.
Contact: ukmesh@proton.me

## 1. Error diagnostics telemetry

**What is stored** (per event): kind (error/warning/unhandledrejection/crash),
truncated message (≤500 chars) and stack (≤4000 chars), page pathname only (no
query string), userAgent, HMAC-hashed source IP. No raw IPs, no account IDs,
no cookies, no localStorage in the telemetry path.

**Lawful basis — legitimate interest (Art 6(1)(f)).** Three-part test:

- **Purpose test:** operating a reliable public service requires knowing when
  client-side failures occur; this telemetry has already root-caused real
  production crashes (deck.gl/maplibre, worker-file MIME regressions).
- **Necessity test:** error capture cannot be tied to a logged-in account
  (the site has no general auth), so pseudonymous event capture is the
  minimum effective means; fields are bounded and truncated.
- **Balancing test:** data is pseudonymous (hashed IP, no identifiers), kept
  30 days, rate-limited (10/min/IP) and sampled (20/hr/source), self-hosted
  (no third-party processor → no DPA required), no profiling. Individual
  impact is minimal; service reliability benefit is direct.

**Retention:** 30 days, enforced by the operational retention loop
(`DATA_LIFECYCLE_RETENTION_ENABLED=true` + `DATA_LIFECYCLE_RETENTION_TARGETS`).
**Erasure:** individuals cannot be mapped to rows; the retention job IS the
erasure mechanism (documented as such).

## 2. Mesh packet database

The service stores radio traffic observed on the MeshCore network.

| Data | Retained | Rationale |
|---|---|---|
| `packets` — message content + raw bytes + metadata | **30 days** (timescaledb retention job) | network diagnostics; content is not kept long-term |
| `packet_decryptions` — full decrypted messages | **30 days** (operational retention loop) | diagnostics only |
| `packet_paths` — path metadata (node IDs, timing, signal, hashes) | **indefinite, content-stripped** | raw pathing analytics is the product |
| `multibyte_path_facts`, `multihop_paths_weekly`, `path_*_priors` — derived path stats | indefinite | derived, pseudonymous |
| `packet_daily_stats` / `packet_hourly_stats` | indefinite | anonymous aggregates |

**Lawful basis — legitimate interest (Art 6(1)(f)).**

- **Purpose test:** operating and improving the network (pathing, coverage,
  diagnostics) requires the packet data; the owner portal requires status data.
- **Necessity test:** content is deleted after 30 days; long-term retention is
  limited to path metadata with no message content and no coordinates, which
  is the minimum needed for pathing analytics.
- **Balancing test:** pseudonymous node identifiers only (linkable to an
  individual solely when a node owner registers in the portal); no content in
  long-lived tables; no individual profiling; disclosed in the privacy notice.

**Design rules (binding):**

- Path/long-lived tables NEVER gain `payload`, `raw_hex`, or coordinate
  columns. Content-bearing tables always have a retention policy.
- Decryption outputs land only in short-retention tables (30 days).
- New derived tables get a lifecycle policy at creation.
- Erasure (Art 17): owner-portal-linked nodes are mappable — node-scoped
  deletion procedure in the ops playbook (rows by src/rx node id across
  packets, packet_paths, decryptions, path facts).

## 3. Cookies

LocalStorage-only choices (cookie consent, theme). No third-party cookies.
PECR does not apply to the mesh radio data (not a public electronic
communications network).
