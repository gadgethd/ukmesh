# CI smoke failure — analysis (Hermes watch, 2026-09-19)

**Context.** Branch `fix/health-latency-bundle`; failing run 35437969785, job "Workers and Compose", step "Build every application image and smoke an empty-volume stack" (failed 10:48:18Z). Recorded while completing the leftover items after the session was cut off at the Codex quota cap.

**Failure.** The smoke stack's `up --no-build --wait` attempted to start `tagger-worker`, but its image was never built in the job:

```
10:48:02Z  tagger-worker            Warning pull access denied for meshcore-analytics-tagger-worker, repository does not exist or may require 'docker login'
10:48:18Z  Container meshcore-ci-35437969785-1-tagger-worker-1  Error response from daemon: No such image: meshcore-analytics-tagger-worker:local
```

`up --wait` then aborted; the `trap cleanup EXIT` tore the project down; the step exited 1.

**Cause.** The `tagger-worker` service (docker-compose.yml:282 — `image: ${TAGGER_WORKER_IMAGE:-meshcore-analytics-tagger-worker:local}`, build context `./tagger-worker`) is part of the dev-profile stack (no `profiles:` gate). The CI step builds an explicit list — `backend app-ukmesh website-ukmesh website-dev mesh-health-check mosquitto-reloader link-worker hopreach` — which does not include `tagger-worker`, and the subsequent `up` runs with `--no-build`, so the image cannot exist on the runner. `ci.yml` contains no tagger references at all: the service arrived with the tags work in the un-pushed four-commit base set and the CI inventory was never updated for it.

**Classification: INHERITED.** The branch does not touch `.github/` or any compose file (`git diff --name-only 1d86e04..HEAD` — none); the identical failure would occur on the base. No gate threshold is relaxed by the fix — the fix simply completes the build inventory.

**Fix (this branch).** Add `tagger-worker` to the step's build list in `.github/workflows/ci.yml`. Follow-up note for the release path: `release.yml` should gain the tagger-worker image when the tagger is due to ship — not required for this bundle (handoff: no tagger deploy needed).

**Evidence.** Raw job log: run 35437969785, job 105883806327 (excerpts quoted above).
