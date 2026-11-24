# Integrated service decisions

## Feed ownership

- The Beacon directory and conversation already own cursor paging, packet deduplication, region changes and scroll anchoring. Build a small Beacon entry around that view. The full Beacon app also creates packet and analytics routes and subscribes to events those routes need. The public entry uses one channel subscription and its own query cache.
- The UKMesh Packet Feed owns packet, watch and path behaviour. Keep it in the UKMesh website at `/feed/packets`. Keep the old feed active while the Beacon read and live contracts are being verified.
- Beacon public routes cannot inherit the broad LAN proxy. A known decryption key or one-byte channel hash cannot authorize publication. The public gateway must validate the exact channel and observation against UKMesh visibility, and it must treat enrichment and old cached data as subject to revocation.

## Map ownership

- CoreScope currently uses host-root URLs while UKMesh owns `/api`, `/ws`, terrain, RF coverage and the installed app’s service worker. Move the fork’s public client URLs behind one deployment config and give the gateway explicit routes. Keep authority and acquisition separate. The public server must use the approved projection, and the browser must expire the previous generation when the authority changes.

## Health Check ownership

- The deployed pin, fork main and upstream have diverged. Verify the security behaviour on the merged source before refactoring. The root build currently copies an external lockfile and overlays four files over the image, making source identity and UI changes hard to verify together. Build the fork source and use one authoritative lockfile after its replacements pass.
