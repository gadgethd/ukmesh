import { pool, query } from '../db/index.js';
import { loadSpamMessageConfig } from '../spam/config.js';
import { resolveSpamOrigin } from '../spam/spamResolver.js';
import { sanitizeOrigin, type PublicIncident } from '../spam/sanitize.js';

// ---------------------------------------------------------------------------
// Re-resolve the ORIGIN of already-stored spam incidents using relay paths.
//
// Older incidents were persisted before path-based origin resolution (or
// outside the analyzer window), so they kept the vaguer observer-signal
// estimate. This re-runs the path resolver on each incident's stored member
// packets and updates its origin in place — no re-clustering.
//
//   node dist/tools/reresolveSpamOrigins.js            # dry run
//   node dist/tools/reresolveSpamOrigins.js --apply    # write updates
// ---------------------------------------------------------------------------

interface IncidentRow {
  incident_key: string;
  network: string;
  message_count: number;
  origin_region: string | null;
  origin_level: string | null;
  public_json: PublicIncident;
}

async function main(): Promise<void> {
  const apply = process.argv.includes('--apply');
  const cfg = loadSpamMessageConfig();

  const incRes = await pool.query<IncidentRow>(
    `SELECT incident_key, network, message_count, origin_region, origin_level, public_json
       FROM spam_message_incidents
      ORDER BY message_count DESC`,
  );
  console.log(`[reresolve] ${incRes.rows.length} incidents, apply=${apply}`);

  let updated = 0;
  for (const row of incRes.rows) {
    const memRes = await pool.query<{ packet_hash: string }>(
      `SELECT packet_hash FROM spam_message_members WHERE incident_key = $1`,
      [row.incident_key],
    );
    const hashes = memRes.rows.map((r) => r.packet_hash);
    if (hashes.length === 0) {
      console.log(`  ${row.incident_key} (${row.message_count}) — no stored members, skip`);
      continue;
    }

    let origin = null;
    try {
      origin = await resolveSpamOrigin(hashes, row.network, query, cfg);
    } catch (err: unknown) {
      console.log(`  ${row.incident_key} — resolve error: ${err instanceof Error ? err.message : err}`);
      continue;
    }

    if (!origin) {
      console.log(
        `  ${row.incident_key} (${row.message_count}) — no path consensus, keep ${row.origin_region} (${row.origin_level})`,
      );
      continue;
    }

    console.log(
      `  ${row.incident_key} (${row.message_count}) — ${row.origin_region} (${row.origin_level}) ` +
        `-> ${origin.region} (${origin.level}, conf ${origin.confidence.toFixed(2)}, ${origin.radiusKm}km)`,
    );

    if (apply) {
      const publicJson = { ...row.public_json, origin: sanitizeOrigin(origin, cfg) };
      await pool.query(
        `UPDATE spam_message_incidents
            SET origin_lat = $2, origin_lon = $3, origin_radius_km = $4, origin_region = $5,
                origin_confidence = $6, origin_level = $7, public_json = $8, updated_at = NOW()
          WHERE incident_key = $1`,
        [
          row.incident_key,
          origin.lat,
          origin.lon,
          origin.radiusKm,
          origin.region,
          origin.confidence,
          origin.level,
          JSON.stringify(publicJson),
        ],
      );
      updated += 1;
    }
  }

  console.log(`[reresolve] ${apply ? `updated ${updated}` : 'dry run only; rerun with --apply'}`);
}

main()
  .catch((err: unknown) => {
    console.error('[reresolve] failed:', err instanceof Error ? err.message : err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
