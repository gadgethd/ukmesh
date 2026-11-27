# Generic new-device owner linking (#425)

## Decision and current coverage

Keep discovery advisory and require operator approval for new grants. A generic
unattended grant service is not a small change: there is no enrollment request
state, bootstrap owner session, approval API, or device proof-of-possession
protocol here. Log claims supply candidate fingerprints for the operator.
Granting a claimed key requires explicit confirmation of its full fingerprint.

The task brief reports an external `simeon` watcher as the only scheduled
auto-link coverage. That scheduler is not represented in this repository and
was not inspected on the VPS. The repository does have generic **operator-run**
helpers; those do not establish generic unattended coverage:

| Current hook | Actual behavior |
| --- | --- |
| `scripts/newuser.sh: main`, `discover_node_key`, `extract_node_key_from_logs` | Initial provisioning accepts explicit keys or waits for fresh user-correlated broker logs. `--watch` resumes a credential-only account or retries verification with the same confirmed keys. `--link` adds a device to an existing owner, with an option to discover its key. All are bounded, operator-invoked runs. |
| `backend/src/tools/confirmOwnerGrants.ts` | Records exact `operator-database` grants with an audit entry and verifies the running reconciler's applied generation. It leaves the deployment running and keeps prior `operator-config` grants intact. |
| `backend/src/mqtt/brokerLog.ts: parseMosquittoConnection`, `parseOwnerNodeTopic` | Parse authenticated username/client association and claimed exact topic keys. Client IDs and topics are publisher-controlled. |
| `backend/src/mqtt/connectionMonitor.ts: observeBrokerLogLine`, `scanRange` | A continuous, bounded, audit-only monitor; exact-key denials go to `recordUntrustedMqttNodeObservation`, not owner grants. Bare default-root denials have no full key. |
| `backend/src/owner/ownerAuth.ts: recordUntrustedMqttNodeObservation` | Retains bounded claims in `mqtt_node_logins`. It cannot turn them into ownership. |
| `backend/src/owner/ownerAccess.ts: autoLinkOwnerNodeIds` | Despite its name, reconciles existing operator-authorized grants, invalidates the owner cache, then resolves verified node IDs. It does not discover or approve new devices. |
| `backend/src/owner/ownerService.ts: authenticateOwner`, `getSessionDashboard` | Authentication requires broker credentials; the normal owner session additionally requires a linked node present in the dashboard. A credential-only user has no normal session. |
| `backend/src/owner/ownerAuth.ts: getOwnerNodeIdsForUsername`, `getOwnerAuthorizationSnapshot` | Only active, verified, unrevoked `operator-config` or `operator-database` grants authorize access. |
| `backend/src/owner/ownerAclReconciler.ts: reconcileOwnerAuthorization` | Synchronizes configured grants, renders exact per-key ACLs, validates and applies them, reloads the broker, and records readback state. |

For today's operator workflow, take the full public key directly from the user
and supply it to the helper with one `--confirm-owner-grant` per key. A
username-correlated discovered topic helps the operator identify a candidate.
Only an explicit approval grants access. If ACL verification is pending,
re-run the approval with the same keys. Use `--watch` for the initial grant
and `--link` for additional keys. Retries preserve the credential and audit
record. The command reports success after the running reconciler records the
applied ACL generation and the broker ACL contains the approved grants.

## Concrete follow-up: generic enrollment with operator approval

1. Add a separate enrollment service and request table in the owner-auth DB
   (`backend/src/db/owner-auth.sql`, applied idempotently by
   `initOwnerAuthDb`; keep this separate from the primary DB migration ledger).
   Store request ID, MQTT username, full claimed key, source, timestamps,
   status (`pending`, `approved`, `rejected`, `revoked`), approving actor, and
   reason. Uniquely deduplicate pending `(username, key)` pairs; cap and expire
   requests per account. Never store a password or private key.
2. At `registerOwnerRoutes` in `backend/src/api/routes/owner.ts`, add dedicated
   enrollment endpoints that recheck MQTT credentials using
   `createMqttCredentialVerifier`. Issue a short-lived, purpose-limited bootstrap
   session for enrollment only, with CSRF and credential-generation checks.
   Do not weaken `requireOwnerSession`, `authenticateOwner`, or owner data APIs
   to grant dashboard access to an unlinked account. Permit entry of a full
   public key even when the default topic contains none. Existing owners can
   request another device through the same service with their normal session.
3. Optionally show `mqtt_node_logins` candidates from the existing monitor to
   the requesting account and operator. Label all keys **unverified**. Never
   choose the first prefix match, infer an exact key from a short client ID,
   or approve based on a denial alone. Correlations must handle reconnects,
   client-ID reuse, disconnects, out-of-order logs and stale evidence.
4. Add an operator-authenticated approval command or route next to the owner
   authorization inventory tooling. It must review full-key evidence, check
   existing owners/conflicts, and reject inactive accounts and tombstoned grants
   unless the operator explicitly authorizes reactivation/reauthorization.
   Reuse `provisionConfirmedOwnerGrants` in `backend/src/owner/ownerAuth.ts`
   for ordinary approvals. It validates account and grant state, records the
   approval in `owner_grant_audit`, and makes repeated approvals idempotent.
   Request approval and grant publication need a transaction or a recoverable
   queue.
5. Report **grant recorded, ACL pending** until the running reconciler's
   applied and readback generations match the desired artifact. Retain the
   request and last verified ACL for retry.
6. Invalidate `invalidateOwnerNodeIdCache(username)` and the service dashboard
   cache when publishing a grant. `createOwnerService().clearOwnerSession(username)`
   already clears that user's dashboard and cached node data and invalidates
   the owner node-ID cache; reuse or extract that invalidation without clearing
   the user's authentication cookie.
   Prompt the user to reconnect and sign in after a valid status/packet arrives.
   No per-account backend recreation or external watcher is needed in this
   service path. `planOperatorConfiguredOwnerGrantSync` preserves active
   `operator-database` grants, while removed `operator-config` grants have
   separate generation/grace semantics; preserve that distinction.

Truly unattended approval requires an additional protocol: an account-bound,
expiring, single-use nonce signed by the device's identity key, verified against
the requested full public key, with replay protection and explicit handling of
already-owned and revoked keys. The existing firmware log envelope does not
provide that proof. Defining firmware support and a new verified-grant method
is a separate change; do not send private keys to the website.

## Acceptance checks for the follow-up

- Any newly provisioned username can request its first device or an additional
  device without a username-specific scheduler. Bare default-root publishes
  still give setup help, without inventing a candidate key.
- A spoofed topic/client ID, short prefix, another account's claim, rejected
  request, inactive account, or revoked grant cannot create authorization.
- Requests survive timeout/restart; duplicate submissions and approvals are
  idempotent. Reauthorization requires an explicit operator decision and audit.
- Broker credential changes revoke bootstrap sessions. Normal owner routes
  retain verified-grant checks; unlinked users cannot read node data.
- ACL failures and concurrent config reconciliation cannot lose existing grants
  or claim an enrollment is ready before verified readback. New account/node
  visibility is demonstrated end to end using an isolated broker and DB.
