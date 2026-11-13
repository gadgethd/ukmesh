/** The canonical view's exact representative/aggregate projection for a set of
 * candidate IDs. Filter the complete identity, never individual members, so
 * network, private-name and representative rules remain the caller's decision.
 */
export function identityNodeSourceSql(candidateIdsSql: string): string {
  return `(
    SELECT identity.*
    FROM (SELECT DISTINCT candidate.node_id FROM (${candidateIdsSql}) candidate ORDER BY candidate.node_id) members
    CROSS JOIN LATERAL core.lookup_node_identity(members.node_id) identity
  )`;
}

/** Most identities have one raw row. Read those directly, and run the complete
 * ranking/aggregate projection only for aliases and normalisation collisions.
 * The collision branch deliberately supports legacy mixed-case/space IDs.
 */
function identityReadParts(candidateIdsSql?: string, pointRows = false) {
  // With a unique stored key, a normalization collision must include a legacy
  // key. Probe that small partial index instead of grouping the entire table.
  const ctes = `WITH duplicate_ids AS MATERIALIZED (
      SELECT DISTINCT UPPER(BTRIM(legacy.node_id)) AS node_id
      FROM (SELECT node_id FROM core.nodes WHERE node_id<>UPPER(BTRIM(node_id)) OFFSET 0) legacy
      WHERE EXISTS (
        SELECT 1 FROM core.lookup_node_members(UPPER(BTRIM(legacy.node_id))) other
        WHERE other.node_id<>legacy.node_id
      )
    )${candidateIdsSql ? `, lookup_candidates AS MATERIALIZED (${candidateIdsSql})` : ''}`;
  const nodeId = 'UPPER(BTRIM(n.node_id))';
  // Observer sets benefit from indexed raw-row hydration. Broad regional
  // reads retain an ordered bulk scan; neither path truncates the candidates.
  const nodeRows = candidateIdsSql && pointRows ? `(
      SELECT point.* FROM (SELECT DISTINCT node_id FROM lookup_candidates) candidate
      CROSS JOIN LATERAL (
        SELECT * FROM core.lookup_node_members(candidate.node_id) OFFSET 0
      ) point
    )` : 'core.nodes';
  const single = `SELECT ${nodeId} AS node_id, n.name, n.lat, n.lon, n.last_seen,
      COALESCE(n.is_online, FALSE) AS is_online,
      n.hardware_model, n.firmware_version, n.public_key, n.created_at,
      n.iata, n.role, COALESCE(n.advert_count, 0)::integer AS advert_count,
      n.elevation_m, n.network, n.last_predicted_online_at, n.last_path_evidence_at,
      n.last_mqtt_observer_seen_at, n.last_rx_at, n.last_status_at, n.observer_iata,
      ARRAY[n.node_id] AS identity_source_ids, 1::integer AS identity_member_count
    FROM ${nodeRows} n
    WHERE ${nodeId} NOT IN (
      SELECT source_node_id FROM core.node_identity_aliases
      UNION SELECT canonical_node_id FROM core.node_identity_aliases
    )
    AND ${nodeId} NOT IN (SELECT node_id FROM duplicate_ids)
    ${candidateIdsSql && !pointRows ? `AND (${nodeId} IN (SELECT node_id FROM lookup_candidates)) IS TRUE` : ''}`;
  const multiple = `SELECT grouped.* FROM ${identityNodeSourceSql(`
      SELECT node_id FROM (
        SELECT canonical_node_id AS node_id FROM core.node_identity_aliases
        UNION SELECT node_id FROM duplicate_ids
      ) multiple_members
      ${candidateIdsSql ? 'WHERE (node_id IN (SELECT node_id FROM lookup_candidates)) IS TRUE' : ''}
    `)} grouped`;
  return { ctes, single, multiple };
}

export function publicIdentityNodeSourceSql(candidateIdsSql?: string): string {
  const { ctes, single, multiple } = identityReadParts(candidateIdsSql);
  return `(${ctes} ${single} UNION ALL ${multiple})`;
}

/** Sparse searches hydrate only their matches. Broad terms retain the bulk
 * projection; neither branch truncates candidates or changes public limits. */
export function candidateIdentityNodeSourceSql(candidateIdsSql: string): string {
  return `(
    WITH search_ids AS MATERIALIZED (${candidateIdsSql})
    SELECT narrow.* FROM ${identityNodeSourceSql('SELECT node_id FROM search_ids')} narrow
      WHERE (SELECT COUNT(*) FROM search_ids) <= 128
    UNION ALL
    SELECT broad.* FROM ${publicIdentityNodeSourceSql('SELECT node_id FROM search_ids')} broad
      WHERE (SELECT COUNT(*) FROM search_ids) > 128
  )`;
}

/** Push the requested top-k into disjoint identity groups AFTER all filters
 * and complete alias aggregation. The first k rows of their sorted union are
 * necessarily contained in the first k rows of each group. This also defers
 * timestamp formatting until only the requested rows remain.
 */
export function pagedIdentityNodeSourceSql(where: string, limit: string, candidateIdsSql?: string, pointCandidates = true, pointRows = false): string {
  const parts = identityReadParts(candidateIdsSql ? 'SELECT node_id FROM page_candidates' : undefined, pointRows);
  const page = (source: string, condition = where) => `SELECT n.* FROM (${source}) n WHERE ${condition}
    ORDER BY n.node_id LIMIT ${limit}`;
  const bulk = `${parts.ctes} (${page(parts.single)}) UNION ALL (${page(parts.multiple)})`;
  if (!candidateIdsSql) return `(${bulk})`;
  if (!pointCandidates) return `(WITH page_candidates AS MATERIALIZED (${candidateIdsSql})
    SELECT broad.* FROM (${bulk}) broad)`;
  return `(
    WITH page_candidates AS MATERIALIZED (${candidateIdsSql})
    (${page(`SELECT * FROM ${identityNodeSourceSql('SELECT node_id FROM page_candidates')} matched`,
      `${where} AND (SELECT COUNT(*) FROM page_candidates)<=128`)})
    UNION ALL
    SELECT broad.* FROM (${bulk}) broad WHERE (SELECT COUNT(*) FROM page_candidates)>128
  )`;
}
