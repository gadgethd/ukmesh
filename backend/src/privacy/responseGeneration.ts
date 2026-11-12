/** A fresh authority read for delivery of derived public snapshots. Pending
 * rematerialization must close this gate before the worker advances generation. */
export async function readPublicResponseGeneration(
  query: (sql: string) => Promise<{ rows: Array<{ generation: string }> }>,
): Promise<number> {
  const result = await query(`SELECT current.generation::text AS generation
    FROM ops.public_visibility_state current
    JOIN ops.packet_visibility_materialization_state materialized USING (singleton)
    WHERE current.singleton = TRUE
      AND materialized.visibility_generation = current.generation
      AND NOT EXISTS (
        SELECT 1 FROM ops.privacy_rematerialization_queue
        WHERE status IN ('pending', 'processing', 'failed')
      )`);
  const generation = Number(result.rows[0]?.generation);
  if (!Number.isSafeInteger(generation) || generation < 1) {
    throw new Error('PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE');
  }
  return generation;
}
