import { createHash, randomUUID } from 'node:crypto';

/** Stable across a restart for an undecodable, hashless MQTT envelope. */
export function fallbackPacketIdentity(payload: Buffer): string {
  return createHash('sha256').update('ukmesh:undecodable-envelope:v1\0').update(payload).digest('hex').toUpperCase();
}

export function ramMqttDeliveryKey(input: {
  clientId: string; messageId?: number; qos: number; topic: string;
  payload: Buffer; observationKey: string;
}): string {
  // QoS 0 has no packet identifier or broker redelivery contract. Give each
  // delivered envelope its own admission claim; RF dedupe remains independent.
  if (input.qos === 0) return `q0:${randomUUID()}`;
  if (input.qos !== 1 || !Number.isInteger(input.messageId)
    || input.messageId! < 1 || input.messageId! > 65_535) {
    throw new Error('MQTT_RAM_DELIVERY_ID_MISSING');
  }
  return createHash('sha256').update(JSON.stringify([
    input.clientId, input.messageId, input.topic, input.payload.toString('hex'), input.observationKey,
  ])).digest('hex');
}

/** Fields that distinguish one observer's RF sighting from another. */
export type ObservationIdentity = {
  packetHash: string;
  rxNodeId: string | null;
  topic: string;
  network: string;
  hopCount: number | null;
  routeType: number | null;
  pathHashes: string[] | null;
  pathHashSizeBytes: number | null;
  direction: 'rx' | 'tx' | null;
};

function normalizedPath(pathHashes: string[] | null): string[] | null {
  return pathHashes?.map((hash) => hash.trim().toUpperCase()) ?? null;
}

export function observationIdentityKey(observation: ObservationIdentity): string {
  return JSON.stringify([
    observation.packetHash,
    observation.rxNodeId,
    observation.topic,
    observation.network,
    observation.hopCount,
    observation.routeType,
    normalizedPath(observation.pathHashes),
    observation.pathHashSizeBytes,
    observation.direction,
  ]);
}

/** The DUP probe must match the same sighting claimed by the live handler. */
export async function packetAlreadyPersisted(
  query: (sql: string, params: unknown[]) => Promise<{ rows: Array<{ present: boolean }> }>,
  observation: ObservationIdentity,
): Promise<boolean> {
  const result = await query(
    `SELECT EXISTS (
       SELECT 1 FROM packets
        WHERE packet_hash = $1
          AND rx_node_id IS NOT DISTINCT FROM $2::text
          AND topic = $3
          AND network = $4
          AND hop_count IS NOT DISTINCT FROM $5::int
          AND route_type IS NOT DISTINCT FROM $6::int
          AND path_hashes IS NOT DISTINCT FROM $7::text[]
          AND path_hash_size_bytes IS NOT DISTINCT FROM $8::int
          AND (payload->>'direction') IS NOT DISTINCT FROM $9::text
          AND time > NOW() - INTERVAL '30 days'
     ) AS present`,
    [
      observation.packetHash,
      observation.rxNodeId,
      observation.topic,
      observation.network,
      observation.hopCount,
      observation.routeType,
      normalizedPath(observation.pathHashes),
      observation.pathHashSizeBytes,
      observation.direction,
    ],
  );
  return Boolean(result.rows[0]?.present);
}
