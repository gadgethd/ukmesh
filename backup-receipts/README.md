# Restore receipt mount

The live backend mounts this directory read-only. Production automation writes
only these public verification artifacts here after a successful isolated
restore drill:

- `latest.json`
- `latest.json.sig`

The verify key is **not** stored here. It is provisioned separately and pinned
by sha256 fingerprint (`RESTORE_RECEIPT_VERIFY_KEY_SHA256`), because a receipt
that carries its own verify key verifies itself.

Private decryption and signing keys must never be stored in this repository or
mounted into the application.
