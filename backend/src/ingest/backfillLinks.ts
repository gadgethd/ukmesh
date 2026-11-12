import { query } from '../db/runtime.js';
import { decodePacketCompat } from '../mqtt/decodePacket.js';
import { buildChannelEntries, buildCombinedKeyStore } from '../mqtt/channelRegistry.js';

export async function backfillHistoricalLinks(
  queueFn: (
    packetHash: string,
    rxNodeId: string,
    srcNodeId: string | undefined,
    path: string[],
    hopCount: number | undefined,
    pathHashSizeBytes: number | undefined,
  ) => void | Promise<unknown>,
  options: { windowEnd?: Date; batchSize?: number } = {},
): Promise<void> {
  const keyStore = buildCombinedKeyStore(buildChannelEntries(process.env['MESHCORE_CHANNEL_SECRETS']));
  const windowEnd = options.windowEnd ?? new Date();
  const batchSize = Math.min(10_000, Math.max(100, Math.floor(options.batchSize ?? 2_000)));
  let cursor = '';
  let queued = 0;
  while (true) {
    const res = await query<{
      packet_hash: string; rx_node_id: string; src_node_id: string | null; hop_count: number | null; raw_hex: string;
    }>(
      `SELECT DISTINCT ON (packet_hash)
         packet_hash, rx_node_id, src_node_id, hop_count, raw_hex
       FROM mesh.packets
       WHERE time < $1
         AND packet_hash > $2
         AND rx_node_id IS NOT NULL
         AND raw_hex IS NOT NULL
         AND raw_hex != ''
       ORDER BY packet_hash, time DESC
       LIMIT $3`,
      [windowEnd, cursor, batchSize],
    );
    if (res.rows.length === 0) break;
    for (const row of res.rows) {
      let compat: ReturnType<typeof decodePacketCompat>;
      try {
        compat = decodePacketCompat(row.raw_hex, keyStore);
      } catch {
        // Skip undecipherable packets
        continue;
      }
      if (compat.metadataValid && (compat.pathHashSize ?? 1) > 1 && compat.pathHashes && compat.pathHashes.length > 0) {
        // Admission failures are operational failures, not decode failures.
        // Let the privileged caller abort rather than publish a partial graph.
        await queueFn(
          row.packet_hash,
          row.rx_node_id,
          row.src_node_id ?? undefined,
          compat.pathHashes,
          compat.pathHashCount,
          compat.pathHashSize,
        );
        queued++;
      }
    }
    cursor = res.rows.at(-1)!.packet_hash;
    if (res.rows.length < batchSize) break;
  }
  console.log(`[app] historical link backfill: queued ${queued} packets`);
}
