# Broker denied-publish triage — 2026-10-23 — #269 / #213

Branch: `burn/ukmesh-broker-20261023`. Starting commit:
`73ab7d4fc63e4968a5fd46af022696ad69d16d9b`.

## Findings and disposition

The two named families have different causes. **ESP32_a806F8 publishes an
unsupported secondary `/raw` topic; fix the publisher without expanding the
ACL. The historical meshmonitor EXT failure was incomplete owner onboarding
on supported packet/status topics; the current ACL already grants that key.**
The old operations note incorrectly described both families as `/raw` noise.

Exact observer keys:

| Name | Public key |
| --- | --- |
| Mick / ESP32 | `8CA23B78908F366D2D9F63EFE4A5726D685332FD3EC9AFF1B3B6CFF22EC2AE8A` |
| Nick / meshmonitor | `67969D8FF8A2DE6FFF873638CCAB3F70E38111BE3BD599F75E9D21D907B217B5` |

The exact ESP32 denied topic is `meshcore/LBA/<Mick key>/raw`. The historical
meshmonitor denied topics are `meshcore/EXT/<Nick key>/packets` and
`meshcore/EXT/<Nick key>/status`, substituting the full keys above.

## Fresh broker evidence

Read-only SSH access to `projects-vps` used the existing
`meshcore-infra-mosquitto-1` container to grep its mounted logs and ACL. Direct
host-volume reads were denied and `sudo -n` required interactive authentication;
no permissions were changed. Compressed archives were decompressed to stdout
and filtered, without extracting files on the VPS. No deployment, reload,
restart, publish probe, live configuration edit, or database operation ran.

All four available `.gz` archives and a current-log snapshot were queried.
The filtered records span **2026-10-20 23:17:24 through 2026-10-23 14:20:55 UTC**.
These are retained-file counts, not lifetime totals or a current rate.

| Source under `/mosquitto/log/` | ESP32 `/raw` | meshmonitor denied | Other denied |
| --- | ---: | ---: | ---: |
| `mosquitto.log.20261021T151701Z.gz` | 901 | 0 | 2,273 |
| `mosquitto.log.20261022T051702Z.gz` | 435 | 0 | 3,495 |
| `mosquitto.log.20261022T181702Z.gz` | 1,020 | 0 | 2,996 |
| `mosquitto.log.20261023T081701Z.gz` | 0 | 0 | 2,942 |
| `mosquitto.log` snapshot | 0 | 0 | 962 |
| **Total** | **2,356** | **0** | **12,668** |

Every ESP32 denial has client ID `ESP32_a806F8` and the exact `/raw` topic
above. Its first/last captured denials are **2026-10-20 23:20:23 / 2026-10-22
17:23:24 UTC**. Zero later denials does not establish that its publisher has
been corrected; it may have stopped publishing.

The additional family is client `meshcore_B397703F0C0C95EB_1`: **12,410**
denials on bare `meshcore`, plus **258** on `meshcore/neighbors`. Both lack the
required IATA/key path. This is a client topic-root misconfiguration, also
covered by the new telemetry. Its operator should set the base topic to
`meshcore/<actual-IATA>/<its-full-64-character-public-key>` and publish the
supported suffixes; verify its full identity rather than deriving a grant from
the client ID prefix.

Current meshmonitor client ID is `meshmonitor-observer-nevrg6ol`; connection
records identify username `nick`. Accepted archive records include:

```text
1792646263: Received PUBLISH from meshmonitor-observer-nevrg6ol (d0, q0, r0, m0, 'meshcore/EXT/67969D8FF8A2DE6FFF873638CCAB3F70E38111BE3BD599F75E9D21D907B217B5/packets', ... (422 bytes))
1792646364: Received PUBLISH from meshmonitor-observer-nevrg6ol (d0, q0, r1, m0, 'meshcore/EXT/67969D8FF8A2DE6FFF873638CCAB3F70E38111BE3BD599F75E9D21D907B217B5/status', ... (448 bytes))
1792646295: Received PUBLISH from ESP32_a806F8 (d0, q0, r0, m0, 'meshcore/LBA/8CA23B78908F366D2D9F63EFE4A5726D685332FD3EC9AFF1B3B6CFF22EC2AE8A/packets', ... (615 bytes))
```

Live ACL grep found `user mick` at line 372 and `user nick` at line 439, each
with eight exact-key write rules: production and test scopes, each covering
`packets`, `status`, `neighbors`, and `neighbours`. Nick's production rule has
`meshcore/+/<Nick key>/packets` (and corresponding status/neighbor rules), so
`EXT` is covered. No policy extension is needed for that topic shape.

## Historical meshmonitor evidence and limits

The September log files are outside current retention. Shared-memory source
`viking://user/default/sessions/20260913_180654_2ab96433/history/archive_001/messages.jsonl`,
tool record `call_00_40IWtp8hOqTXObKb6w8A3600`, preserves an SSH broker-log
`tail -30` sample. It contains **three packet denials and one status denial**
from `meshmonitor-observer-qy92lten`:

```text
1788720614: Denied PUBLISH from meshmonitor-observer-qy92lten (d0, q0, r1, m0, 'meshcore/EXT/67969D8FF8A2DE6FFF873638CCAB3F70E38111BE3BD599F75E9D21D907B217B5/status', ... (378 bytes))
1788720634: Denied PUBLISH from meshmonitor-observer-qy92lten (d0, q0, r0, m0, 'meshcore/EXT/67969D8FF8A2DE6FFF873638CCAB3F70E38111BE3BD599F75E9D21D907B217B5/packets', ... (456 bytes))
1788720653: Denied PUBLISH from meshmonitor-observer-qy92lten (d0, q0, r0, m0, 'meshcore/EXT/67969D8FF8A2DE6FFF873638CCAB3F70E38111BE3BD599F75E9D21D907B217B5/packets', ... (525 bytes))
1788720661: Denied PUBLISH from meshmonitor-observer-qy92lten (d0, q0, r0, m0, 'meshcore/EXT/67969D8FF8A2DE6FFF873638CCAB3F70E38111BE3BD599F75E9D21D907B217B5/packets', ... (429 bytes))
```

Embedded broker timestamps put this sample at **2026-09-06 18:50:14–18:51:01
UTC**, despite its September 13 session provenance. Four is the sample count,
not a historical total. The same session's onboarding record (#212) describes
`nick` authenticating without an owner account, ACL stanza, or configured
grant. That explains supported-topic rejection; today's exact-key ACL and
accepted messages show that condition is no longer present in the observed
retained window. Do not classify a future `/packets` denial as harmless `/raw`
noise or automatically grant a key from a log claim.

## Reproduction and preserved evidence

From the worktree root, the current-log extraction was:

```sh
ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes projects-vps "docker exec meshcore-infra-mosquitto-1 grep -E 'Denied PUBLISH|New client connected.*(ESP32_a806F8|meshmonitor-observer)' /mosquitto/log/mosquitto.log" > .ukmesh-tools/broker-20261023.log
```

Each archive used this command, substituting `STAMP` with each table filename's
timestamp. Output redirection is local to this worktree:

```sh
ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes projects-vps "docker exec meshcore-infra-mosquitto-1 sh -c 'gzip -cd /mosquitto/log/mosquitto.log.STAMP.gz | grep -E \"Denied PUBLISH|New client connected.*(ESP32_a806F8|meshmonitor-observer)\"'" > .ukmesh-tools/broker-denials-STAMP.log
ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes projects-vps "docker exec meshcore-infra-mosquitto-1 grep -n -B 1 -A 12 -E '^user (nick|mick)$|67969D8FF8A2DE6FFF873638CCAB3F70E38111BE3BD599F75E9D21D907B217B5|8CA23B78908F366D2D9F63EFE4A5726D685332FD3EC9AFF1B3B6CFF22EC2AE8A' /mosquitto/config/acl"
```

Accepted samples used the October 22 18:17 archive with
`grep -m 6 -E "Received PUBLISH from (ESP32_a806F8|meshmonitor-observer)"`,
plus `grep -m 1 -E "Received PUBLISH from meshmonitor-observer.*status"`.
An auxiliary read of broker stdout since October 1 found only meshmonitor
connections, with no denials; it overlaps retained files and was not added to
their counts.

Filtered-file SHA-256 values (local files under ignored `.ukmesh-tools/`):

| File | SHA-256 |
| --- | --- |
| `broker-denials-20261021T151701Z.log` | `45a5cf81f38071d97489c5596b993304ed8cf7308c80532a14d2f87ee279ff06` |
| `broker-denials-20261022T051702Z.log` | `05b42f5ab7c2e32076a8a6a9eb2292d8a9ea25937cdfb856235ddc9c621b1fc7` |
| `broker-denials-20261022T181702Z.log` | `a16eefd00317a3d52730ded950b2f245caf716ead11be620743fcae252897397` |
| `broker-denials-20261023T081701Z.log` | `2159a309d784a960734cc1ffb75cd65499c17c861ff4f059587c79edda77a3e8` |
| `broker-20261023.log` | `bc39482ed9e511219a52147b24878b450b80353e2b2e28c488743529a579492a` |

## Observable fix and operator handoff

The monitor previously filtered denial lines through the owner-topic parser,
silently dropping `/raw` and malformed roots. It now parses denials separately
from optional owner-node claims. Every parsed denial increments
`meshcore_mqtt_denied_publishes_total{kind,phase}`, with six fixed suffix
buckets and separate history/live phases. JSON warning records carry client,
topic, optional username, and count; at most 64 distinct groups per scan and
512 characters per identifying field. Omitted details remain counted and have
an explicit overflow warning. Polling retains partial lines until their
newline arrives, including splits inside UTF-8 characters.

Only supported owner topics with existing connection context enter the
unchanged untrusted database audit; this change cannot create owner grants or
alter the ACL. History may replay the last 50 MB at startup; query `phase="live"`
for recurrence. Telemetry is a process-local log observation count, not a
durable broker accounting ledger. Log access/logging remains a prerequisite.

[Operator instructions](../operations.md#denied-publish-on-raw-269--213) give
Mick the exact secondary topic to disable while preserving packet JSON and its
`raw` field. Nick needs no current client change; any renewed supported-topic
denials require review of the actual username/key and approved owner grant,
followed by fresh accepted-message verification. No live fix was applied.

## Verification

All commands below ran from `backend/` against this worktree; no live database
or broker was used by tests. Dependencies were installed with
`npm_config_cache="$PWD/node_modules/.cache/npm" npm ci --ignore-scripts --no-audit --no-fund`
(185 packages; approximately 94 MB including the local cache).

| Command | Result |
| --- | --- |
| `node --import tsx --test src/mqtt/brokerLog.test.ts src/mqtt/connectionMonitor.test.ts src/mqtt/topic.test.ts src/mqtt/aclManager.test.ts src/metrics.test.ts` | **34/34 pass**, zero skipped or failed. |
| `env -u DATABASE_URL -u OWNER_DATABASE_URL -u TEST_ANALYSIS_DATABASE_URL -u TEST_VACUUM_DATABASE_URL -u TEST_INGEST_DATABASE_URL -u TEST_REDIS_URL -u TEST_REDIS_PASSWORD -u TEST_NODE_CLEANUP_POSTGRES_MODULE -u TEST_NODE_CLEANUP_PGLITE_MODULE TMPDIR="$PWD/../.ukmesh-tools/tmp" npm_config_cache="$PWD/node_modules/.cache/npm" timeout 600s npm test` | **588 total: 585 pass, 0 fail, 3 existing opt-in skips**, 97.5 seconds; synthetic alert firing/recovery receiver passes. |
| `npm run typecheck && npm run build` | Both pass. An intermediate Buffer generic inference error was fixed by explicitly typing the pending buffer; final checks are green. |
| `node --import tsx ../.ukmesh-tools/broker-count.mjs` | New parser accepts **all 15,024 captured denial records**, zero unparsed denials; totals and file hashes match the tables above. This local evidence checker is ignored, not a runtime dependency. |
| `git diff --check` (worktree root) | Pass. |

Regression tests cover both exact topic families, unsupported/malformed topics,
grant boundaries, missing/expired usernames, duplicate aggregation, bounded
details and metric labels, historical/live separation, partial UTF-8 lines,
and audit failures that must not erase denial telemetry. The three full-suite
skips are existing opt-in read-only database EXPLAIN checks. No migration,
frontend change, new container, or large artifact was required.
