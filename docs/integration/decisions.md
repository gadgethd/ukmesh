# Integrated service decisions

## Feed ownership

- The Beacon directory and conversation already own cursor paging, packet deduplication, region changes and scroll anchoring. Build a small Beacon entry around that view. The full Beacon app also creates packet and analytics routes and subscribes to events those routes need. The public entry uses one channel subscription and its own query cache.
- The UKMesh Packet Feed owns packet, watch and path behaviour. Keep it in the UKMesh website at `/feed/packets`. Keep the old feed active while the Beacon read and live contracts are being verified.
- Beacon public routes cannot inherit the broad LAN proxy. A known decryption key or one-byte channel hash cannot authorize publication. The public gateway must validate the exact channel and observation against UKMesh visibility, and it must treat enrichment and old cached data as subject to revocation.
- The restricted history source lives in the UKMesh qualification scripts. Beacon owns frame decoding and reconciles its short hash with the full UKMesh packet identity. The reader binds its cursor to the network set and privacy generation and checks the authority again after reading. Two hearings with equal times and content remain distinct by observation UUID. Tests use an owned database and a disposable read-only role. A public channel policy, authenticated message projection, and live delivery are required before routing user traffic.

## Map ownership

- CoreScope currently uses host-root URLs while UKMesh owns `/api`, `/ws`, terrain, RF coverage and the installed app’s service worker. Move the fork’s public client URLs behind one deployment config and give the gateway explicit routes. Keep authority and acquisition separate. The public server must use the approved projection, and the browser must expire the previous generation when the authority changes.
- The isolated preview edge separates static files, CoreScope’s narrow read routes, and the UKMesh runtime configuration. It checks the projection before forwarding CoreScope data or accepting a WebSocket upgrade. Cookies and authorization headers stay within their original services. Keep RF archives outside the preview edge. The shared archive manifest contains per-repeater location and tile references and must have a generation-fenced public projection before it can serve the new app. Keep the existing application ingress until the full map and installed-app checks pass.

## Health Check ownership

- The deployed pin, fork main and upstream had diverged. The merge kept the deployed origin and session checks, and the refactor gave WebSocket transport its own module. The app and share screens use the same guided layout. A restored version-1 store verified historical targets and receipts in the built image.
- The Health Check gitlink is the source pin for DEV, CI and release builds. BuildKit fetches that exact commit from the fork before installing dependencies from the fork’s lockfile. This binds the image source to its revision label even when a local checkout contains additional assets. DEV and release builds verify the selected pin. Remove the external lockfile and the image overlays together.
