# MeshCore channel decryption

How the UK Mesh site decrypts group-text (packet type 5) traffic live, and
how historical traffic was retroactively decrypted.

## Key store

- `backend/src/mqtt/channelRegistry.ts` is the single source of truth:
  - `VALIDATED_CHANNELS` — default secrets baked into the image (39 entries
    incl. Public; recovered 2026-08-06 and validated to decrypt real
    human-readable group text).
  - `buildCombinedKeyStore()` — merges baked defaults with
    `MESHCORE_CHANNEL_SECRETS` (env, comma-separated `name:hex` or bare hex;
    dedupes by secret) for secrets that shouldn't be committed.
  - `buildSummary()` / `identifyChannel()` — shared by ingest and offline
    tools.
- ⚠️ The repo is **public**: keys in `channelRegistry.ts` are public. Keep
  non-derivable/community keys in the env var only.
- Reading env at startup only — after a change:
  `docker compose -f docker-compose.yml -f docker-compose.live.yml up -d --no-deps --force-recreate backend`.

## Decryption format

GRP_TXT payload = `[channel hash 1B][MAC 2B][AES-128-ECB ciphertext]`.
Hashtag channel keys are derivable: `sha256("#name")[:16]`. The public
channel key (`8b3387e9c5cdea6ac9e5edbaa115cd72`, hash byte `11`) is published
in MeshCore docs. Other derivation schemes exist in the wild (sha1/md5
variants, direct ASCII passphrases).

## Stored decrypted data

- Live ingest stores decrypted content inline in `payload.decrypted`
  (message/sender/timestamp/flags) and `_summary` =
  `[ChannelName] sender: message`.
- Historical decryption lives in the `packet_decryptions` side table
  (migration 035), filled by the backfill tool. The feed joins it with
  `COALESCE(p.payload->>'_summary', pd.summary)`.
- ⛔ Never bulk-UPDATE the `packets` hypertable — TimescaleDB plans a seq-scan
  over every chunk. INSERT into the side table instead.

## Backfill tool

`node dist/tools/backfillDecrypt.js` (run inside the backend container).
Two-phase keyset-paginated scan over `packets_hash_idx` (~1,250 hashes/s).
2026-08-06 run: 387,161 packets processed, 173,778 decrypted.

## Feed integration

- `GET /api/feed/messages?channel=<scope>&limit<=50` — up to 50 unique
  historical messages per channel (90-day bound, dedup by packet hash).
- The feed sidebar lists every decrypted channel
  (`MESSAGE_SCOPE_CHANNELS` in `frontend/src/pages/ukmesh/feedModel.ts`);
  new channels need a frontend rebuild to appear as filters.

## Validation gate (Ben, 2026-08-06)

Never deploy a recovered key whose plaintext isn't verified USEFUL — sane
epoch timestamps + printable UTF-8 on a time-spread sample. 1-byte hash
buckets collide across channels (MAC pass rate can reach ~97% on junk);
always validate before `.env` deploy. Honest decrypt coverage measured at
**~43.5%** of the 90-day volume (raw hash-bucket sums overcount).

## Out-of-band keys

`~/ukmesh/meshcore-discord-bot/.env` holds `NORTHEAST_CHANNEL_SECRET`
(== #northeast) and `THENORF_CHANNEL_SECRET` (hash 8A) — the discord bot
forwards those channels. Check it before attacking new keys.
