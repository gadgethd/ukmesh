# Deferred E2E coverage for 2.0

The feed and owner tests below retain their `test.fixme` markers while their
Stage 2 adoption/parity work is completed. Removing a marker requires executing
the test against the intended production artifact and resolving its assertions.

| Test | Why deferred | Stage 2 item |
|---|---|---|
| `frontend/test/e2e/feed-virtualizer.spec.ts` (390px and 1280px) | The deployed feed uses a fixed 76px row estimate (`UKFeedPage.tsx` `virtualRows`). `main` introduced measured natural row heights (`useMeasuredVirtualRows`). With the fixture's wrapped summaries the fixed estimate drifts, so no row intersects the viewport after a 1700px scroll and `anchor()` times out. | Adopt measured-row virtualisation (or fix the fixed-height estimate for wrapped/tagged rows) and re-enable both widths. Real-world risk: spacer drift with long messages. |
| `frontend/test/e2e/owner.spec.ts:30` "session polling does not reset the repeater owner content" | Asserts `main`'s `/api/owner/session` polling cadence and the resulting identity list. Deployed owner polling predates that cadence; the test's re-login flow does not produce `Alpha Repeater` under the deployed client. | Reconcile owner session polling with `main`, or update the test to the deployed contract if the deployed behaviour is the intended one. |
| `frontend/test/e2e/owner.spec.ts:90` "owner map construction remains one across repeated live polls" | Same root cause: asserts `main`'s owner live-poll cadence and map lifecycle instrumentation; the deployed owner client does not produce the expected DOM/state. | Same Stage 2 owner-polling parity item. |
| `frontend/test/e2e/dashboard.spec.ts` "RF coverage remains available with 3D terrain" | Active; the prior CI timeout remains historical evidence. | Qualified on 2026-11-12 against the emitted 2.0 app on desktop/mobile, with real DEM decoding, successful terrain/RF PNG responses and populated, time-relative nodes. Hidden/unused RF polling and visible node-job completion are also covered. |

Four cases remain deferred: both feed-virtualizer widths and both owner live-poll
cases. The RF/terrain test is active and must stay in the release qualification
suite. Test counts from Stage 1 are historical, not a current full-suite receipt.
