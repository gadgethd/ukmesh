# UK Mesh onboarding review — 2026-10-23

Branch: `burn/ukmesh-onboard-20261023`.
Base: `origin/main` at `73ab7d4fc63e4968a5fd46af022696ad69d16d9b`.
Implementation: `f983a749c603f9077b4852e446127971072911ca`.
Brief: `BRIEF-ukmesh-onboard.md`, read in full; #427 and #425.

## Outcome

**#427 implemented.** The passive Mosquitto log monitor now recognises denied
publishes to the default `meshcore` topic and its direct supported children,
including those with no public key. After a connection associates the exact
client ID with an authenticated MQTT username, a bounded in-memory hint records
the denial reason and exact repair template. Login failures for credential-only
users show it after successful broker authentication; existing owners see it
above their nodes on login/session refresh. Invalid credentials and other
accounts cannot read that hint. The monitor still cannot grant ownership.

The browser regression test also exposed an existing frontend problem:
`checkedJson` discarded HTTP error bodies, replacing the useful reason with
`Request failed (403)`. An opt-in JSON error-message path, enabled for owner login
only, now preserves bounded user-facing messages and the HTTP status. HTML,
malformed bodies and invalid error objects retain the generic message.

The [public Install page source](../../frontend/src/pages/ukmesh/UKInstallPage.tsx)
now has an `#mqtt-observer` troubleshooting section, linked from login errors
and dashboard notices. The [onboarding guide](../mqtt-onboarding.md) provides
the exact flasher fields, serial commands, readback checks, effective topic
example, and the separate owner-grant check. `newuser` prints the warning before
discovery as well as after provisioning, and emits the runtime token template
instead of baking a specific node identity into the firmware setting. README
provisioning instructions now match the helper's actual credential-first
discovery behavior.

**#425 assessed; precise design delivered.** Generic operator-run `newuser
--watch`/`--link` already exists, but continuous broker discovery is audit-only.
`autoLinkOwnerNodeIds` reconciles approved grants; it does not approve newly
observed keys. There is no generic enrollment/approval service in this tree.
Building one safely requires request state, enrollment-only authentication,
operator approval or device proof of possession, revocation handling, and ACL
readback coordination, so it is outside a small implementation. The
[design note](../designs/generic-owner-linking.md) identifies exact existing
functions and files, proposed state transitions, cache invalidation, failure
recovery and acceptance checks. No partial linking feature was introduced.
The brief's external username-specific watcher was not independently inspected.

## Denial path and exact fix

The firmware topic builders use `meshcore` literally; that default does not
contain the segments required by the exact-key ACL renderer. A rejected publish
never reaches backend MQTT ingest, so payload ingestion cannot diagnose it.
The configured broker log already enables debug denial events and timestamps,
and the backend already mounts that log read-only and starts the monitor when
MQTT ingest is enabled. No broker configuration change is needed for this code.

Set IATA to the device location and store
`meshcore/{IATA}/{PUBLIC_KEY}/packets` in the UK Mesh broker slot's Topic root.
The firmware expands both tokens at runtime and substitutes `/status` for its
status publishes. This behavior was checked read-only against
[the firmware source](https://github.com/gadgethd/MeshCore-MQTT/blob/df46fb8d39e2970faf60269cd42ed8e88ad15874/examples/simple_repeater/MqttReporter.cpp).
An identity change still needs a matching server-side grant for the new full key.

## Verification

All commands ran locally with the installed Node `v22.22.2`; no live credentials,
DB, broker, VPS, or hardware were used. Exit status was **0** for each final
command below. Shell provisioning tests use their fake Docker implementation.

| Exact command (repository root unless a directory is stated) | Result |
| --- | --- |
| `npm --prefix backend run typecheck` | Passed. |
| In `backend`: `node --import tsx --test src/mqtt/brokerLog.test.ts src/mqtt/connectionMonitor.test.ts src/mqtt/onboardingDiagnostics.test.ts src/mqtt/aclManager.test.ts src/owner/ownerService.test.ts src/api/routes/owner.test.ts` | 32 passed; parser, correlation, expiry, cache cap, auth isolation, login/session responses and existing ACL behavior. |
| In `backend`: `node --import tsx --test src/cache/policyRegistry.test.ts src/owner/ownerGrantReconciliation.test.ts` | 5 passed; bounded cache registration and grant reconciliation semantics. |
| `npm --prefix backend test` | 586 tests: 583 passed, 3 skipped, 0 failed; synthetic local alert receiver check also passed. |
| `npm --prefix backend run build` | Passed. |
| `npm --prefix backend run contract:check` | Passed; generated contract current, 71 API and 11 operator routes. |
| `TMPDIR="$PWD/.ukmesh-tools/onboard/tmp" bash scripts/test-newuser.sh` | Passed; existing concurrency/rollback tests plus literal firmware template and default-root warning assertions. |
| `bash -n scripts/newuser.sh scripts/test-newuser.sh` | Passed. |
| In `frontend`: `node --import tsx --test src/utils/api.test.ts src/pages/owner/ownerPortalModel.test.ts` | 9 passed after the login error-body fix. |
| In `frontend`: `TMPDIR="$PWD/../.ukmesh-tools/onboard/tmp" npm test` | 157 tests: 151 passed, 6 skipped, 0 failed. |
| `npm --prefix frontend run build` | Passed; existing large-chunk advisory only. |
| In `frontend`: the Playwright command below | 2 passed. Real browser checks denial text, setup navigation and exact commands, existing owner notice, and removal on session refresh. |
| `git diff --check` | Passed. |

```sh
cd frontend
TMPDIR="$PWD/../.ukmesh-tools/onboard/tmp" PLAYWRIGHT_PORT_BASE=4973 node node_modules/@playwright/test/cli.js test owner.spec.ts --project=public-desktop --workers=1 --timeout=60000 --grep 'unlinked owner|linked owner sees a recent'
```

An initial four-worker browser attempt timed out in setup. Serial execution
exposed the discarded 403 reason, which was fixed;
the final two-test run passed in 7.5 seconds. The full E2E suite and physical
firmware application were not claimed as verified.

Documentation check, also exit **0**:

```sh
python3 - <<'PY'
from pathlib import Path
import re
files = ['docs/mqtt-onboarding.md', 'docs/designs/generic-owner-linking.md', 'docs/runbook-owner-auth.md']
checked = 0
for name in files:
    source = Path(name)
    for target in re.findall(r'\[[^\]]*\]\(([^)]+)\)', source.read_text()):
        if '://' in target or target.startswith('#'):
            continue
        destination = (source.parent / target.split('#', 1)[0]).resolve()
        assert destination.exists(), (name, target)
        checked += 1
for name in ['docs/mqtt-onboarding.md', 'frontend/src/pages/ukmesh/UKInstallPage.tsx', 'scripts/newuser.sh']:
    assert 'meshcore/{IATA}/{PUBLIC_KEY}/packets' in Path(name).read_text(), name
print(f'PASS: {checked} relative Markdown links resolve; all 3 setup surfaces show the exact firmware template')
PY
```

Result: **3 relative links resolved; all 3 setup surfaces carry the exact template**.
The browser additionally verified `/install#mqtt-observer` and its repair text.

## Limits and stretch disposition

Hints expire from broker-event time after 15 minutes, ignore stale/future
timestamps, retain at most 4,096 accounts/4 MiB of diagnostic values, and require
a retained authenticated-client correlation. Missing logs, disabled ingest,
expired/evicted correlations, log history outside the scan window, or separate
backend processes can mean no hint. Reconnect and retry; absence of a hint is
not a successful-publish signal. A repaired device's recent notice can remain
until expiry, as its label and timestamp explain. No arbitrary denial reason
was added to MQTT QoS 0 acknowledgements; the portal interprets broker evidence.

Read-only #269 check: the existing operations ruling still applies. The ACL
renderer and ingest topic parser support `packets`, `status`, `neighbors`, and
`neighbours`, not `/raw`; packet bytes are a JSON field on `/packets`. The new
monitor regression also checks that `/raw` does not become a claimed owner key.
No live denial counts were collected and no ACL was widened.

No deployment, live service restart/stop, VPS access, live configuration edit, secret
read, live migration, other-worktree modification, or agent spawning occurred.
The branch is based on the required `origin/main`. This review is committed as
the final handoff artifact; the final response supplies its full commit SHA.
