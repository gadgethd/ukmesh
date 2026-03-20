# Contributing

## Structural expectations

- routes should stay thin
- services own orchestration
- repositories own SQL
- worker RF math should stay separate from queue orchestration
- frontend map builders should stay separate from `MapLibreMap.tsx`

## Repository hygiene

Do not commit:
- secrets
- credentials
- private database dumps
- personal or personally identifiable information
- operational traces containing sensitive infrastructure details
- artifacts that could be used for abuse or malicious access

If in doubt, keep it out of Git and add it to `.gitignore`.

## DB changes

- schema creation goes in base schema or migrations
- heavy historical fixes go in maintenance scripts, never startup

## Testing expectations

Before finishing a refactor or behavior change:
- build `backend`
- build `frontend`
- if worker code changed, run Python syntax checks
- rebuild affected containers and check `http://localhost:3000/healthz`
