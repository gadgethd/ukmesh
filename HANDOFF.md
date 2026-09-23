# B3 packet-sharing handoff

## Status

Backend packet-sharing routes, contracts, persistence, forwarding, and tests are committed locally in `fbf9803` (`feat(owner): add owner-controlled packet sharing`). No migration was applied, no service was restarted, and nothing was pushed.

## WIP inventory

The packet-sharing slice rescued from the main checkout contains migrations 052–054 for destination/rule/delivery tables, destination metadata, and default-off rules; owner GET/POST routes and contracts; broker-secret encryption; destination configuration and delivery-queue services; MQTT forwarding and worker wiring; and route/config/forwarder tests. The main checkout also has mixed public-feed, tagger, owner-portal, and unrelated backend work.

The main checkout and its WIP remain untouched. The packet-sharing backend copy there becomes redundant after this change is merged and deployed. The mixed owner-portal UI edits were not included in the backend scope in this brief and need separate disposition if they are not handled elsewhere.

## Migration decision

Kept the supplied `052_owner_packet_sharing.sql`, `053_owner_packet_share_destination_metadata.sql`, and `054_owner_packet_share_default_off.sql` names. No duplicate prefix exists for these files in the target worktree, so no renumbering was needed. Prefix 055 is reserved by `055_message_tags.sql` on `fix/health-latency-bundle`; the separate untracked `055_public_feed.sql` in the main checkout was not copied.

## Tests

- `cd backend && npm test` — passed, 341 tests.
- `cd backend && npm run typecheck` — passed.
- `git diff --check` — passed.

The worktree had no installed dependencies. Tests reused the existing main-checkout `backend/node_modules` through a temporary symlink in this worktree; the symlink was removed afterward. No tests wrote to a live database.

## Gated items and open questions

- Ben owns migration application and deployment.
- The target worktree does not contain the `OWNER_PACKET_SHARE_*` Compose passthroughs or `owner_packet_share_deliveries` in the retention target list described by the brief. No `.env` or config files were changed under the VPS constraint. Before deployment, Ben must confirm the runtime passes `OWNER_PACKET_SHARE_ENCRYPTION_KEY` to the backend and includes `owner_packet_share_deliveries` in lifecycle retention targets. Poll interval and batch size have code defaults.
- Confirm whether the packet-sharing owner-portal UI edits in the main checkout are tracked in another change; they were left untouched and are not in `fbf9803`.
