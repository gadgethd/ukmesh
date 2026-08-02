# Security finding closure

This report revalidates the security and privacy findings in `plan.md` against
the implementation that is prepared for deployment. A finding is closed only
when the vulnerable behavior is removed or an explicitly recorded product
decision and compensating controls make the reported condition non-secret or
non-reachable. Deployment receipts provide the separate runtime proof for the
exact images placed in service.

## Finding disposition

| ID | Disposition | Implementation and regression evidence |
| --- | --- | --- |
| SEC-01 | Closed | `backend/src/security/outboundWebhook.ts` resolves and pins public HTTPS destinations, rejects non-public and rebinding results, and sends through a bounded durable outbox. `outboundWebhook.test.ts` and `owner/alertRules.test.ts` cover address classes, redirects, retry and delivery bounds. |
| SEC-02 | Closed | The backend has no Docker socket mount, runs as UID/GID 1000 with a read-only filesystem, dropped capabilities and `no-new-privileges`. `mosquitto-reloader` owns the narrowly scoped ACL reload operation. `mqtt/aclManager.test.ts`, the Compose policy audit and the live non-root/read-only checks cover the boundary. |
| SEC-03 | Closed | `queue/publisher.ts` uses the versioned viewshed-v2 admission contract with atomic count and byte limits, bounded repair and explicit rejection/coalescing. `publisher.integration.test.ts` and `viewshed-worker/tests/test_viewshed_queue_v2.py` cover producer/consumer agreement, concurrency, crash recovery and randomized repair. |
| SEC-04 | Closed | Both Nginx entry points overwrite the forwarded identity for `/ws` and `/ws/`; `trustedProxy.ts` accepts it only from an exact configured proxy peer. `trustedProxy.test.ts`, the browser E2E suite and the deployment proxy-quota probe cover spoofing, per-client quotas and distinct clients. |
| SEC-05 | Closed | Link history is served by the scoped repository in `repositories/productFeatures.ts`, with the same network/private-node predicate and cache key in either endpoint direction. API contract, network-filter and privacy tests cover private, cross-network, reversed and missing pairs. |
| SEC-06 | Closed | Browser diagnostics are stored as bounded diagnostic signals and are not authoritative service-health inputs. Synthetic and backend checks remain authoritative. `health/statusIntegrity.test.ts`, limiter tests and alert firing/recovery drills cover spoofed, repeated and real failures. |
| SEC-07 | Closed | `Dockerfile.mesh-health-check` requires a full immutable Git commit, verifies the fetched commit before copying executable content, installs from the lockfile and records the pin in the image. Compose supplies the reviewed pin and the release workflow records the resulting digest and provenance. |
| SEC-08 | Closed by hardening | `requireLocalOnly` rejects forwarding metadata from untrusted peers and rejects any public address in the peer/forwarded chain. Operator session route tests cover direct, trusted-proxy and spoofed public traffic. Public proxy ingress remains separated from the loopback operator listener. |
| SEC-09 | Closed | Decoded-path prefix candidates are filtered through the selected network/privacy scope before resolution. `statsRepository.test.ts`, `statsService.test.ts`, `networkScope.test.ts` and decoded-path masking tests cover cross-network and private candidates. |
| SEC-10 | Closed | `scripts/generate-observer-key.ts` creates its key directory as `0700`, creates private material as `0600`, refuses unsafe paths and uses atomic replacement. `scripts/test-generate-observer-key.sh` verifies modes and failure behavior. |
| SEC-11 | Closed | README and operations documentation route every public Cloudflare origin through its Anubis sidecar. Compose exposes application origins on loopback and keeps the documented sidecar aliases stable. |
| SEC-12 | Rejected as a secret finding | `docs/architecture.md` records the literal as the documented MeshCore public-channel key. It is protocol interoperability material, not a deployment credential. Private channel secrets remain environment-only and are excluded from repository output and logs. |
| SEC-13 | Closed | Anonymous coordinate release is centralized in server-side privacy predicates and DTO/repository boundaries before caching or fanout. REST, WebSocket, exports and derived-path tests cover private prefixes and placeholder/exact-coordinate suppression; owner/operator DTOs retain separately authorized exact data. |
| SEC-14 | Closed by bounded design | Planned coverage uses unguessable SHA-256 handles, TTL cleanup, feature gating, per-source rate limits, durable global count/cost limits and isolated bounded worker execution. `plannedCoverage.test.ts` covers each admission edge and legacy handle compatibility. |
| SEC-15 | Closed | Frontend dependencies were upgraded and the production dependency audit is clean at high severity. CI runs the lockfile audit, unit/E2E suites and bundle build from a clean install. |
| SEC-16 | Closed by recorded contract | `docs/architecture.md` records that the public application excludes network `test`, while the separate development build is fixed to it; the Feed `Test` selector is only a content-channel label. Network, request-scope, WebSocket privacy and repository tests enforce that separation. |

## Release-time revalidation

For each deployment, the signed local deployment receipt must bind the source
revision to exact image IDs and record:

1. no Docker socket mount on an Internet-facing service;
2. non-root, read-only, capability-dropped runtime policy;
3. `/ws` forwarding and quota isolation through the public proxy;
4. public REST, WebSocket and browser privacy checks;
5. health alert firing and recovery without diagnostic authority;
6. clean production dependency/image scans or time-bounded reviewed waivers.

Any failed release-time check reopens the associated finding and requires
rollback to the recorded compatible image set.
