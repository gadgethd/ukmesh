# Deferred E2E coverage (Stage 1)

These tests come from `main` and assert `main`'s implementation. Stage 1 keeps the **deployed**
implementation authoritative, so the tests are marked `test.fixme` rather than rewritten. Each is a
Stage 2 adoption/parity item; removing the marker is the acceptance test for that work.

| Test | Why deferred | Stage 2 item |
|---|---|---|
| `frontend/test/e2e/feed-virtualizer.spec.ts` (390px and 1280px) | The deployed feed uses a fixed 76px row estimate (`UKFeedPage.tsx` `virtualRows`). `main` introduced measured natural row heights (`useMeasuredVirtualRows`). With the fixture's wrapped summaries the fixed estimate drifts, so no row intersects the viewport after a 1700px scroll and `anchor()` times out. | Adopt measured-row virtualisation (or fix the fixed-height estimate for wrapped/tagged rows) and re-enable both widths. Real-world risk: spacer drift with long messages. |
| `frontend/test/e2e/owner.spec.ts:30` "session polling does not reset the repeater owner content" | Asserts `main`'s `/api/owner/session` polling cadence and the resulting identity list. Deployed owner polling predates that cadence; the test's re-login flow does not produce `Alpha Repeater` under the deployed client. | Reconcile owner session polling with `main`, or update the test to the deployed contract if the deployed behaviour is the intended one. |
| `frontend/test/e2e/owner.spec.ts:90` "owner map construction remains one across repeated live polls" | Same root cause: asserts `main`'s owner live-poll cadence and map lifecycle instrumentation; the deployed owner client does not produce the expected DOM/state. | Same Stage 2 owner-polling parity item. |
| `frontend/test/e2e/dashboard.spec.ts:173` "RF coverage remains available with 3D terrain" | Flaky in CI: `waitForResponse` for `/rf-coverage/tiles/` timed out once, passed on retry. | Not a Stage 1 blocker; watch for repeat failures. |

29 E2E tests pass in the stack (public feed, owner map, dashboard smoke). Four specs are deferred:
both feed-virtualizer widths and both owner live-poll specs.
