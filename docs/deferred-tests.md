# Stage 2 E2E qualification for 2.0

The four formerly deferred feed/owner cases are active. They were executed
against the emitted public website on 2026-11-12 with their behavioral
assertions retained. No `test.fixme` markers remain in the E2E suite.

| Test | Resolution and retained coverage |
| --- | --- |
| `frontend/test/e2e/feed-virtualizer.spec.ts` (390px and 1280px) | The feed adopts `useMeasuredVirtualRows` with stable packet keys, observed natural heights and bounded measurement state. Both widths pass wrapping, late observer enrichment, prepends, filtering/empty lists, resize across internal/document scroll modes, bottom reachability and top anchoring. Native scroll anchoring is disabled on the virtual list so it cannot compete with the measured anchor. |
| `frontend/test/e2e/owner.spec.ts` “session polling does not reset the repeater owner content” | Active and passed. Explicit fallback fixtures keep unmocked optional owner endpoints from contacting a developer backend and returning an unrelated 401. The test retains identity/content and live-request-count assertions, waits for live content, and accommodates the shared poller's 10% cadence jitter. |
| `frontend/test/e2e/owner.spec.ts` “owner map construction remains one across repeated live polls” | Active and passed. The original canvas-construction counter still proves that repeated live polls preserve the existing map. The complete five-case owner suite also passes revocation, broker-denial guidance and refreshed diagnostics. |
| `frontend/test/e2e/dashboard.spec.ts` “RF coverage remains available with 3D terrain” | Active. Qualified on 2026-11-12 against the emitted app on desktop/mobile, with real DEM decoding, successful terrain/RF PNG responses and populated, time-relative nodes. Hidden/unused RF polling and visible node-job completion remain covered. |

Keep these scenarios in candidate qualification. Their local receipts do not
replace the final full production-browser matrix; Stage 1 counts and earlier
timeouts remain historical evidence.
