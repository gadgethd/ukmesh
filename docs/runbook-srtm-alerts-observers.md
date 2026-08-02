# SRTM, alerts, and observer operations

## SRTM terrain cache

Both RF workers share the named `srtm_data` volume at `/data/srtm`. Downloads
validate coordinates, status, compressed and decompressed size, and use a
per-tile lock plus atomic replacement. The default cache byte cap is 20 GiB;
link jobs may request at most 64 tiles and viewshed jobs 256.

Inspect usage and recent outcomes:

```bash
docker compose exec -T viewshed-worker sh -c \
  'du -sh /data/srtm; find /data/srtm -maxdepth 1 -type f -name "*.hgt" | wc -l'
docker compose exec -T backend wget -qO- http://127.0.0.1:9091/metrics \
  | grep '^meshcore_srtm_'
```

The worker prunes least-recently-used `.hgt` files after a job and protects
tiles active in that job. To reduce the bound, set `SRTM_CACHE_MAX_BYTES` and
recreate both workers; allow normal pruning to converge. Do not remove the
volume while RF work is active. A corrupted tile should be moved aside only
after stopping both workers; restart them and let the bounded downloader
replace it. Persistent download failures or budget warnings belong in the job
dead-letter reason.

## Monitoring alerts

Alertmanager groups by alert name/severity, sends firing and resolved events to
the internal receiver, and retains its own state for 120 hours. The receiver
stores bounded, rotated summaries in `alert_receiver_data`; optional forwarding
uses `ALERT_FORWARD_URL`.

```bash
docker compose exec -T alertmanager amtool \
  --alertmanager.url=http://127.0.0.1:9093 alert
docker compose exec -T alert-receiver sh -c \
  'tail -n 50 /var/lib/meshcore-alerts/alerts.jsonl'
docker compose logs --since=30m prometheus alertmanager alert-receiver
```

Run `scripts/test-alert-receiver.sh` after configuration changes. Treat a
critical alert as unresolved until its underlying metric is healthy and a
resolved receipt arrives; do not silence a symptom without an incident note.
The alert-specific first responses are in `docs/operations.md`.

## Owner alert delivery

The owner portal shows rule status, sanitized destination host, delivery
attempt history, last success/error, pause reason, dead-letter state, and “send
test.” Destinations are validated both at rule creation and immediately before
delivery. A failure receives at most five exponential-backoff attempts.

For a delivery incident, confirm the rule is still granted to the owner, check
its pause reason and history, send one test delivery, and inspect
`meshcore_owner_alert_*` metrics. Never log or paste the full webhook URL.
Repeated failures must remain dead-lettered until the destination or policy is
fixed; creating duplicate rules is not recovery.

## Observer registration

Submissions enter `pending` and are visible only at
`/observer-registrations`. PII must not be copied to public logs, metrics, or
issues. Normalize and compare the public key and the displayed possible
duplicate before deciding.

- Approve with a required reason after identity and requested IATA review.
- Reject with a required reason and optional duplicate record ID.
- Provision only an approved request using the existing credential/ACL
  workflow.
- Mark notification sent or failed after the provisioning handoff.
- Allow the lifecycle worker to expire untouched requests; do not delete them
  manually.

Every state transition is atomic, idempotent, and records the pseudonymous
operator actor. Registration terminal records retain for 365 days; expired
requests retain for 90 days. Use the operator audit view for an exact history.

## Planned-node publication

Private owner keys and notes remain visible only in `/operations`. Publication
requires an operator-reviewed name, coordinates, optional height/region,
expiry, and reason. `/api/planned-nodes` reads only the separate publication
table and returns its closed DTO with cursor pagination. Unpublish immediately
if a private value or unreviewed coordinate appears; then treat any anonymous
exposure as a privacy incident.
