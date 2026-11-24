# Integrated service decisions

## Feed ownership

- The Beacon directory and conversation already own cursor paging, packet deduplication, region changes and scroll anchoring. Build a small Beacon entry around that view. The full Beacon app also creates packet and analytics routes and subscribes to events those routes need. The public entry uses one channel subscription and its own query cache.
- The UKMesh Packet Feed owns packet, watch and path behaviour. Keep it in the UKMesh website at `/feed/packets`. Keep the old feed active while the Beacon read and live contracts are being verified.
- Beacon public routes cannot inherit the broad LAN proxy. A known decryption key or one-byte channel hash cannot authorize publication. The public gateway must validate the exact channel and observation against UKMesh visibility, and it must treat enrichment and old cached data as subject to revocation.
- The restricted history source lives in the UKMesh qualification scripts. Beacon owns frame decoding and reconciles its short hash with the full UKMesh packet identity. The reader binds its cursor to the network set and privacy generation and checks the authority again after reading. Two hearings with equal times and content remain distinct by observation UUID. Tests use an owned database and a disposable read-only role. A public channel policy, authenticated message projection, and live delivery are required before routing user traffic.
- The private Beacon authority shares the UKMesh channel registry and ready visibility check. Channel names and the one-byte wire hash are unsuitable keys for publication. Export only approved material, identify it by its full key fingerprint, and carry a registry revision alongside the visibility generation. The Beacon credential stays separate from the CoreScope stream credential.

## Map ownership

- CoreScope currently uses host-root URLs while UKMesh owns `/api`, `/ws`, terrain, RF coverage and the installed app’s service worker. Move the fork’s public client URLs behind one deployment config and give the gateway explicit routes. Keep authority and acquisition separate. The public server must use the approved projection, and the browser must expire the previous generation when the authority changes.
- The isolated preview edge separates static files, CoreScope’s narrow read routes, and the UKMesh runtime configuration. It checks the projection before forwarding CoreScope data or accepting a WebSocket upgrade. Cookies and authorization headers stay within their original services. The preview sends RF reads through the candidate projection. That projection matches network tiles to the current generation and rechecks each archived repeater's visibility and coordinates. The old RF mount stays with the legacy app during qualification.
- The cutover previews share the credential-stripping proxy settings. CoreScope requests the existing UKMesh region and path APIs through an explicit allowlist. The UKMesh service worker keeps its existing scope and retains the previous cache generation. It can cache versioned CoreScope scripts and styles. API, RF, and runtime authority responses stay on the network.

## Health Check ownership

- The deployed pin, fork main and upstream had diverged. The merge kept the deployed origin and session checks, and the refactor gave WebSocket transport its own module. The app and share screens use the same guided layout. A restored version-1 store verified historical targets and receipts in the built image.
- The Health Check gitlink is the source pin for DEV, CI and release builds. BuildKit fetches that exact commit from the fork before installing dependencies from the fork’s lockfile. This binds the image source to its revision label even when a local checkout contains additional assets. DEV and release builds verify the selected pin. Remove the external lockfile and the image overlays together.

## DEV qualification

- Beacon readiness needs native connection events, process identity and the current broker epoch. Streaming hours of access logs through Docker repeatedly exhausted the probe budget. Filter the same journal by container and event fields when the service uses journald. The startup deadline and the separate intake, broker and target identity checks still apply.
- A completed companion build can disappear between Docker listing and inspection, aborting a valid transition. Retry only after confirming a listed ID disappeared. Reinspect the entire current set and require exact full IDs before comparing owned service and broker identities.
