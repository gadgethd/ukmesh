import type { QueryResultRow } from 'pg';

export type QueryFn = <T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
) => Promise<{ rows: T[] }>;

export type ObserverRegistrationInput = {
  publicKey: string;
  iata: string;
  name: string | null;
  contact: string;
};

function normalizedText(value: unknown): string {
  return typeof value === 'string' ? value.trim().replace(/\s+/g, ' ') : '';
}

export function normalizeObserverRegistration(value: unknown): ObserverRegistrationInput {
  const body = value as Record<string, unknown> | null | undefined;
  const publicKey = normalizedText(body?.['publicKey']).toUpperCase();
  const iata = normalizedText(body?.['iata']).toUpperCase();
  const name = normalizedText(body?.['name']);
  const rawContact = normalizedText(body?.['contact']);
  const contact = /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(rawContact)
    ? rawContact.toLowerCase()
    : rawContact;
  if (
    !/^[0-9A-F]{64}$/.test(publicKey)
    || !/^[A-Z0-9]{2,8}$/.test(iata)
    || name.length > 100
    || contact.length < 3
    || contact.length > 200
    || /[\u0000-\u001f\u007f]/.test(`${name}${contact}`)
  ) {
    throw new Error('INVALID_OBSERVER_REGISTRATION');
  }
  return { publicKey, iata, name: name || null, contact };
}

export async function observerHealthRows(query: QueryFn, networks: string[]) {
  return query<{
    node_id: string;
    name: string | null;
    lat: number;
    lon: number;
    active_hours: string;
    packets_48h: string;
    unique_src_48h: string;
  }>(
    `WITH observer_activity AS (
       SELECT meshcore_canonical_node_id(p.rx_node_id) AS rx_node_id,
         COUNT(DISTINCT date_trunc('hour', p.time)) AS active_hours,
         COUNT(*) AS packets_48h,
         COUNT(DISTINCT meshcore_canonical_node_id(p.src_node_id))
           FILTER (WHERE p.src_node_id IS NOT NULL) AS unique_src_48h
       FROM packets p
       WHERE p.time > NOW() - INTERVAL '48 hours'
         AND p.network = ANY($1::text[])
         AND p.rx_node_id IS NOT NULL
       GROUP BY p.rx_node_id
     )
     SELECT n.node_id, n.name, n.lat, n.lon,
       oa.active_hours::text, oa.packets_48h::text, oa.unique_src_48h::text
     FROM observer_activity oa
     JOIN node_identity_nodes n ON n.node_id = oa.rx_node_id
     WHERE n.lat IS NOT NULL AND n.lon IS NOT NULL
       AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')`,
    [networks],
  );
}

export async function visibleLinkNodeIds(
  query: QueryFn,
  nodeIds: string[],
  networks: string[],
) {
  return query<{ node_id: string }>(
    `SELECT n.node_id
       FROM node_identity_nodes n
      WHERE n.node_id = ANY($1::text[])
        AND (n.name IS NULL OR n.name NOT LIKE '%🚫%')
        AND (
          n.network = ANY($2::text[])
          OR (
            n.network IS DISTINCT FROM 'test'
            AND EXISTS (
              SELECT 1
                FROM node_identity_sightings sighting
               WHERE sighting.node_id = n.node_id
                 AND sighting.network = ANY($2::text[])
                 AND sighting.last_seen_at > NOW() - INTERVAL '30 days'
            )
          )
        )`,
    [nodeIds, networks],
  );
}

export async function linkHistoryRows(
  query: QueryFn,
  nodeA: string,
  nodeB: string,
  hours: number,
) {
  return query<{
    time: string;
    snr: number | null;
    rssi: number | null;
    path_loss: number | null;
    sample_count: number;
  }>(
    `SELECT reports.last_seen::text AS time, reports.last_snr_db AS snr,
            NULL::double precision AS rssi, links.itm_path_loss_db AS path_loss,
            reports.sample_count
     FROM node_identity_link_radio_reports reports
     LEFT JOIN node_identity_links links
       ON links.node_a_id = reports.node_a_id AND links.node_b_id = reports.node_b_id
     WHERE reports.node_a_id = meshcore_canonical_node_id($1)
       AND reports.node_b_id = meshcore_canonical_node_id($2)
       AND reports.last_seen > NOW() - ($3::text || ' hours')::interval
     ORDER BY reports.last_seen ASC`,
    [nodeA, nodeB, String(hours)],
  );
}

export async function repeaterFirmwareRows(query: QueryFn, networks: string[]) {
  return query<{ hardware_model: string | null; firmware_version: string | null; count: string }>(
    `SELECT COALESCE(hardware_model, 'Unknown') AS hardware_model,
            COALESCE(NULLIF(firmware_version, ''), 'Unknown') AS firmware_version,
            COUNT(*)::text AS count
     FROM node_identity_nodes
     WHERE network = ANY($1::text[])
       AND (role IS NULL OR role = 2)
       AND last_seen > NOW() - INTERVAL '30 days'
       AND (name IS NULL OR name NOT LIKE '%🚫%')
     GROUP BY 1, 2
     ORDER BY COUNT(*) DESC, 1, 2`,
    [networks],
  );
}

export async function submitObserverRegistration(
  query: QueryFn,
  input: ObserverRegistrationInput,
): Promise<{ requestId: string; accepted: boolean }> {
  const result = await query<{ id: string; accepted: boolean }>(
    `WITH inserted AS (
       INSERT INTO observer_registration_requests (
         public_key, iata, display_name, contact, expires_at
       ) VALUES ($1, $2, $3, $4, NOW() + INTERVAL '90 days')
       ON CONFLICT (public_key) DO NOTHING
       RETURNING id
     )
     SELECT id::text, TRUE AS accepted FROM inserted
     UNION ALL
     SELECT existing.id::text, FALSE
       FROM observer_registration_requests existing
      WHERE existing.public_key = $1
        AND NOT EXISTS (SELECT 1 FROM inserted)
     LIMIT 1`,
    [input.publicKey, input.iata, input.name, input.contact],
  );
  const row = result.rows[0];
  if (!row) throw new Error('OBSERVER_REGISTRATION_WRITE_FAILED');
  return { requestId: row.id, accepted: row.accepted };
}
