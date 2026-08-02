# Restore receipt mount

The live backend mounts this directory read-only. Production automation writes
only these public verification artifacts here after a successful isolated
restore drill:

- `latest.json`
- `latest.json.sig`
- `verify.pem`

Private decryption and signing keys must never be stored in this repository or
mounted into the application.
