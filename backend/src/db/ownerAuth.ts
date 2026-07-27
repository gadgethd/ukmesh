import pg from 'pg';
import fs from 'node:fs';
import { randomUUID } from 'node:crypto';
import { resolveDbAssetPath } from './assets.js';

const { Pool } = pg;
const OWNER_DB_NAME = process.env['OWNER_POSTGRES_DB'] ?? 'meshcore_owner_auth';
const ownerDatabaseApplicationName = String(process.env['OWNER_DATABASE_APPLICATION_NAME'] ?? 'meshcore-owner-auth').trim() || 'meshcore-owner-auth';
const ownerAdminDatabaseApplicationName = String(process.env['OWNER_DATABASE_ADMIN_APPLICATION_NAME'] ?? 'meshcore-owner-auth-admin').trim() || 'meshcore-owner-auth-admin';
const ownerDatabaseStatementTimeoutMs = Number(process.env['OWNER_DATABASE_STATEMENT_TIMEOUT_MS'] ?? 30_000);
const mqttAuditMaxNodesPerUser = Math.min(
  4_096,
  Math.max(16, Number(process.env['MQTT_AUDIT_MAX_NODES_PER_USER'] ?? 256) || 256),
);
const mqttAuditRetentionDays = Math.min(
  365,
  Math.max(1, Number(process.env['MQTT_AUDIT_RETENTION_DAYS'] ?? 30) || 30),
);

function getPrimaryDatabaseUrl(): string {
  const raw = String(process.env['DATABASE_URL'] ?? '').trim();
  if (!raw) throw new Error('DATABASE_URL is required');
  return raw;
}
function withDatabaseName(connectionString: string, databaseName: string): string {
  const url = new URL(connectionString);
  url.pathname = `/${databaseName}`;
  return url.toString();
}

function getOwnerDatabaseUrl(): string {
  return String(process.env['OWNER_DATABASE_URL'] ?? '').trim()
    || withDatabaseName(getPrimaryDatabaseUrl(), OWNER_DB_NAME);
}

function getAdminDatabaseUrl(): string {
  return withDatabaseName(getPrimaryDatabaseUrl(), 'postgres');
}

const ownerPool = new Pool({
  connectionString: getOwnerDatabaseUrl(),
  application_name: ownerDatabaseApplicationName,
  max: Number(process.env['OWNER_DATABASE_POOL_MAX'] ?? 3),
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
  statement_timeout: ownerDatabaseStatementTimeoutMs,
  query_timeout: ownerDatabaseStatementTimeoutMs,
});

ownerPool.on('error', (err) => {
  console.error('[owner-auth] unexpected pool error', err.message);
});

function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

async function ensureOwnerDatabase(): Promise<void> {
  const ownerUrl = new URL(getOwnerDatabaseUrl());
  const databaseName = ownerUrl.pathname.replace(/^\//, '').trim();
  if (!databaseName) throw new Error('OWNER_DATABASE_URL is missing a database name');

  const adminPool = new Pool({
    connectionString: getAdminDatabaseUrl(),
    application_name: ownerAdminDatabaseApplicationName,
    max: 1,
    idleTimeoutMillis: 5_000,
    connectionTimeoutMillis: 5_000,
    statement_timeout: ownerDatabaseStatementTimeoutMs,
    query_timeout: ownerDatabaseStatementTimeoutMs,
  });

  try {
    const exists = await adminPool.query<{ exists: number }>(
      'SELECT 1 AS exists FROM pg_database WHERE datname = $1',
      [databaseName],
    );
    if (exists.rowCount && exists.rows[0]?.exists === 1) return;
    await adminPool.query(`CREATE DATABASE ${quoteIdentifier(databaseName)}`);
    console.log(`[owner-auth] created database ${databaseName}`);
  } finally {
    await adminPool.end().catch(() => undefined);
  }
}

export async function initOwnerAuthDb(): Promise<void> {
  await ensureOwnerDatabase();
  const schemaPath = resolveDbAssetPath('owner-auth.sql');
  const sql = fs.readFileSync(schemaPath, 'utf8');
  await ownerPool.query(sql);
  console.log('[owner-auth] schema initialised');
}

export async function getOwnerNodeIdsForUsername(mqttUsername: string): Promise<string[]> {
  const normalized = mqttUsername.trim();
  if (!normalized) return [];
  const res = await ownerPool.query<{ node_id: string }>(
    `SELECT DISTINCT oan.node_id
     FROM owner_account_nodes oan
     JOIN owner_accounts oa ON oa.mqtt_username = oan.mqtt_username
     WHERE oa.is_active = true
       AND oan.mqtt_username = $1
       AND oan.node_id ~ '^[0-9A-F]{64}$'
       AND oan.verified_at IS NOT NULL
       AND oan.verification_method IN ('operator-config', 'operator-database')
       AND oan.revoked_at IS NULL
     ORDER BY oan.node_id ASC`,
    [normalized],
  );
  return res.rows.map((row) => row.node_id);
}

export type OwnerAuthorizationSnapshot = {
  mqttUsername: string;
  isActive: boolean;
  nodeIds: string[];
};

export async function getOwnerAuthorizationSnapshot(): Promise<OwnerAuthorizationSnapshot[]> {
  const result = await ownerPool.query<{
    mqtt_username: string;
    is_active: boolean;
    node_ids: string[];
  }>(
    `SELECT oa.mqtt_username,
            oa.is_active,
            COALESCE(
              ARRAY_AGG(oan.node_id ORDER BY oan.node_id) FILTER (
                WHERE oan.verified_at IS NOT NULL
                  AND oan.verification_method IN ('operator-config', 'operator-database')
                  AND oan.revoked_at IS NULL
                  AND oan.node_id ~ '^[0-9A-F]{64}$'
              ),
              ARRAY[]::text[]
            ) AS node_ids
       FROM owner_accounts oa
       LEFT JOIN owner_account_nodes oan ON oan.mqtt_username = oa.mqtt_username
      GROUP BY oa.mqtt_username, oa.is_active
      ORDER BY oa.mqtt_username ASC`,
  );
  return result.rows.map((row) => ({
    mqttUsername: row.mqtt_username,
    isActive: row.is_active,
    nodeIds: row.is_active ? row.node_ids : [],
  }));
}

export async function getOwnerAuthorizationInventory(): Promise<{
  accounts: Array<{
    mqttUsername: string;
    isActive: boolean;
    nodeId: string | null;
    verificationMethod: string | null;
    verifiedAt: string | null;
    revokedAt: string | null;
  }>;
  observations: Array<{ mqttUsername: string; nodeId: string; lastConnectedAt: string }>;
}> {
  const [accounts, observations] = await Promise.all([
    ownerPool.query<{
      mqtt_username: string;
      is_active: boolean;
      node_id: string | null;
      verification_method: string | null;
      verified_at: string | null;
      revoked_at: string | null;
    }>(
      `SELECT oa.mqtt_username, oa.is_active, oan.node_id, oan.verification_method,
              oan.verified_at, oan.revoked_at
         FROM owner_accounts oa
         LEFT JOIN owner_account_nodes oan ON oan.mqtt_username = oa.mqtt_username
        ORDER BY oa.mqtt_username, oan.node_id`,
    ),
    ownerPool.query<{
      mqtt_username: string;
      node_id: string;
      last_connected_at: string;
    }>(
      `SELECT mqtt_username, node_id, last_connected_at
         FROM mqtt_node_logins
        ORDER BY last_connected_at DESC
        LIMIT 10000`,
    ),
  ]);
  return {
    accounts: accounts.rows.map((row) => ({
      mqttUsername: row.mqtt_username,
      isActive: row.is_active,
      nodeId: row.node_id,
      verificationMethod: row.verification_method,
      verifiedAt: row.verified_at,
      revokedAt: row.revoked_at,
    })),
    observations: observations.rows.map((row) => ({
      mqttUsername: row.mqtt_username,
      nodeId: row.node_id,
      lastConnectedAt: row.last_connected_at,
    })),
  };
}

export async function closeOwnerAuthDb(): Promise<void> {
  await ownerPool.end();
}

export async function ensureOwnerAccount(mqttUsername: string): Promise<void> {
  const normalized = mqttUsername.trim();
  if (!normalized) return;
  await ownerPool.query(
    `INSERT INTO owner_accounts (mqtt_username, is_active, updated_at)
     VALUES ($1, true, NOW())
     ON CONFLICT (mqtt_username)
     DO UPDATE SET is_active = true, updated_at = NOW()`,
    [normalized],
  );
}

export type OwnerGrantVerificationMethod = 'operator-config' | 'operator-database';

type GrantAuditInput = {
  mqttUsername: string;
  nodeId: string;
  eventType: 'grant' | 'revoke' | 'reauthorize';
  source: OwnerGrantVerificationMethod;
  actor: string;
  reason?: string;
  generation?: string;
};

async function insertGrantAudit(
  client: pg.PoolClient,
  input: GrantAuditInput,
): Promise<void> {
  await client.query(
    `INSERT INTO owner_grant_audit (
       event_id, mqtt_username, node_id, event_type, source, actor, reason, generation
     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
    [
      randomUUID(),
      input.mqttUsername,
      input.nodeId,
      input.eventType,
      input.source,
      input.actor,
      input.reason ?? null,
      input.generation ?? null,
    ],
  );
}

function normalizeGrant(mqttUsername: string, nodeId: string): { mqttUsername: string; nodeId: string } {
  const normalizedUsername = mqttUsername.trim();
  const normalizedNodeId = nodeId.trim().toUpperCase();
  if (!normalizedUsername || !/^[A-Za-z0-9_.@-]{1,128}$/.test(normalizedUsername)
    || !/^[0-9A-F]{64}$/.test(normalizedNodeId)) {
    throw new Error('INVALID_OWNER_GRANT');
  }
  return { mqttUsername: normalizedUsername, nodeId: normalizedNodeId };
}

export async function upsertVerifiedOwnerGrant(
  mqttUsername: string,
  nodeId: string,
  options: {
    verificationMethod: OwnerGrantVerificationMethod;
    actor: string;
    reason?: string;
    generation?: string;
    reauthorize?: boolean;
  },
): Promise<void> {
  const grant = normalizeGrant(mqttUsername, nodeId);
  const client = await ownerPool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO owner_accounts (mqtt_username, is_active, updated_at)
       VALUES ($1, TRUE, NOW())
       ON CONFLICT (mqtt_username) DO UPDATE SET is_active = TRUE, updated_at = NOW()`,
      [grant.mqttUsername],
    );
    const existing = await client.query<{ revoked_at: string | null }>(
      `SELECT revoked_at FROM owner_account_nodes
        WHERE mqtt_username = $1 AND node_id = $2
        FOR UPDATE`,
      [grant.mqttUsername, grant.nodeId],
    );
    const wasRevoked = Boolean(existing.rows[0]?.revoked_at);
    if (wasRevoked && !options.reauthorize) throw new Error('OWNER_GRANT_REAUTHORIZATION_REQUIRED');

    await client.query(
      `INSERT INTO owner_account_nodes (
         mqtt_username, node_id, verification_method, verified_at, grant_id,
         revoked_at, revocation_reason, grant_generation, updated_at
       ) VALUES ($1, $2, $3, NOW(), $4, NULL, NULL, $5, NOW())
       ON CONFLICT (mqtt_username, node_id) DO UPDATE SET
         verification_method = EXCLUDED.verification_method,
         verified_at = NOW(),
         grant_id = EXCLUDED.grant_id,
         revoked_at = NULL,
         revocation_reason = NULL,
         grant_generation = EXCLUDED.grant_generation,
         updated_at = NOW()`,
      [grant.mqttUsername, grant.nodeId, options.verificationMethod, randomUUID(), options.generation ?? null],
    );
    await insertGrantAudit(client, {
      ...grant,
      eventType: wasRevoked ? 'reauthorize' : 'grant',
      source: options.verificationMethod,
      actor: options.actor,
      reason: options.reason,
      generation: options.generation,
    });
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export async function syncOperatorConfiguredOwnerGrants(
  grants: Array<{ mqttUsername: string; nodeId: string }>,
  generation: string,
): Promise<void> {
  const normalized = grants.map((grant) => normalizeGrant(grant.mqttUsername, grant.nodeId));
  const desiredKeys = new Set(normalized.map((grant) => `${grant.mqttUsername}\0${grant.nodeId}`));
  const current = await ownerPool.query<{
    mqtt_username: string;
    node_id: string;
    revoked_at: string | null;
    verification_method: string | null;
    grant_generation: string | null;
  }>(
    `SELECT mqtt_username, node_id, revoked_at, verification_method, grant_generation
       FROM owner_account_nodes`,
  );

  for (const grant of normalized) {
    const row = current.rows.find((candidate) =>
      candidate.mqtt_username === grant.mqttUsername && candidate.node_id === grant.nodeId);
    if (row?.revoked_at) continue;
    if (row?.verification_method === 'operator-database') continue;
    if (row?.verification_method === 'operator-config' && row.grant_generation === generation) continue;
    await upsertVerifiedOwnerGrant(grant.mqttUsername, grant.nodeId, {
      verificationMethod: 'operator-config',
      actor: 'config-reconciler',
      generation,
    });
  }
  for (const row of current.rows) {
    if (row.verification_method !== 'operator-config'
      || row.revoked_at
      || desiredKeys.has(`${row.mqtt_username}\0${row.node_id}`)) continue;
    await revokeOwnerGrant(row.mqtt_username, row.node_id, {
      actor: 'config-reconciler',
      reason: 'removed from operator configuration',
      generation,
    });
  }
}

export async function revokeOwnerGrant(
  mqttUsername: string,
  nodeId: string,
  options: { actor: string; reason: string; generation?: string },
): Promise<void> {
  const grant = normalizeGrant(mqttUsername, nodeId);
  const client = await ownerPool.connect();
  try {
    await client.query('BEGIN');
    const updated = await client.query<{ verification_method: OwnerGrantVerificationMethod }>(
      `UPDATE owner_account_nodes
          SET revoked_at = NOW(), revocation_reason = $3, updated_at = NOW()
        WHERE mqtt_username = $1 AND node_id = $2 AND revoked_at IS NULL
      RETURNING verification_method`,
      [grant.mqttUsername, grant.nodeId, options.reason],
    );
    if (updated.rowCount) {
      await insertGrantAudit(client, {
        ...grant,
        eventType: 'revoke',
        source: updated.rows[0]?.verification_method ?? 'operator-database',
        actor: options.actor,
        reason: options.reason,
        generation: options.generation,
      });
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export async function recordUntrustedMqttNodeObservation(mqttUsername: string, nodeId: string): Promise<void> {
  const normalizedUsername = mqttUsername.trim();
  const normalizedNodeId = nodeId.trim().toUpperCase();
  if (!normalizedUsername || !/^[0-9A-F]{64}$/.test(normalizedNodeId)) return;
  const client = await ownerPool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      'SELECT pg_advisory_xact_lock(hashtext($1))',
      [`mqtt-audit:${normalizedUsername}`],
    );
    await client.query(
      `INSERT INTO mqtt_node_logins (mqtt_username, node_id, last_connected_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (mqtt_username, node_id) DO UPDATE SET last_connected_at = NOW()`,
      [normalizedUsername, normalizedNodeId],
    );
    await client.query(
      `DELETE FROM mqtt_node_logins
        WHERE last_connected_at < NOW() - ($1 * INTERVAL '1 day')`,
      [mqttAuditRetentionDays],
    );
    await client.query(
      `WITH ranked AS (
         SELECT mqtt_username,
                node_id,
                ROW_NUMBER() OVER (
                  PARTITION BY mqtt_username
                  ORDER BY last_connected_at DESC, node_id
                ) AS retained_rank
           FROM mqtt_node_logins
          WHERE mqtt_username = $1
       )
       DELETE FROM mqtt_node_logins target
        USING ranked
        WHERE target.mqtt_username = ranked.mqtt_username
          AND target.node_id = ranked.node_id
          AND ranked.retained_rank > $2`,
      [normalizedUsername, mqttAuditMaxNodesPerUser],
    );
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export type OwnerAclStateUpdate = {
  desiredGeneration?: string;
  renderedGeneration?: string;
  appliedGeneration?: string;
  lastError?: string | null;
  verified?: boolean;
};

export async function updateOwnerAclState(update: OwnerAclStateUpdate): Promise<void> {
  await ownerPool.query(
    `UPDATE owner_acl_state SET
       desired_generation = COALESCE($1, desired_generation),
       rendered_generation = COALESCE($2, rendered_generation),
       applied_generation = COALESCE($3, applied_generation),
       desired_at = CASE WHEN $1::text IS NULL THEN desired_at ELSE NOW() END,
       rendered_at = CASE WHEN $2::text IS NULL THEN rendered_at ELSE NOW() END,
       applied_at = CASE WHEN $3::text IS NULL THEN applied_at ELSE NOW() END,
       last_verified_at = CASE WHEN $5::boolean THEN NOW() ELSE last_verified_at END,
       last_error = $4,
       updated_at = NOW()
     WHERE singleton = TRUE`,
    [
      update.desiredGeneration ?? null,
      update.renderedGeneration ?? null,
      update.appliedGeneration ?? null,
      update.lastError ?? null,
      update.verified ?? false,
    ],
  );
}

export async function getOwnerAclReadiness(): Promise<{
  desiredGeneration: string | null;
  renderedGeneration: string | null;
  appliedGeneration: string | null;
  lastVerifiedAt: string | null;
  lastError: string | null;
}> {
  const result = await ownerPool.query<{
    desired_generation: string | null;
    rendered_generation: string | null;
    applied_generation: string | null;
    last_verified_at: string | null;
    last_error: string | null;
  }>(
    `SELECT desired_generation, rendered_generation, applied_generation,
            last_verified_at, last_error
       FROM owner_acl_state
      WHERE singleton = TRUE`,
  );
  const row = result.rows[0];
  return {
    desiredGeneration: row?.desired_generation ?? null,
    renderedGeneration: row?.rendered_generation ?? null,
    appliedGeneration: row?.applied_generation ?? null,
    lastVerifiedAt: row?.last_verified_at ?? null,
    lastError: row?.last_error ?? null,
  };
}

export async function saveOwnerAclArtifact(input: {
  generation: string;
  rendererVersion: string;
  mode: 'shadow' | 'apply';
  contentSha256: string;
  content: string;
  semantic: unknown;
  validation: unknown;
  applied?: boolean;
}): Promise<void> {
  await ownerPool.query(
    `INSERT INTO owner_acl_artifacts (
       generation, renderer_version, mode, content_sha256, content,
       semantic_json, validation_json, applied_at
     ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8)
     ON CONFLICT (generation) DO UPDATE SET
       mode = EXCLUDED.mode,
       content_sha256 = EXCLUDED.content_sha256,
       content = EXCLUDED.content,
       semantic_json = EXCLUDED.semantic_json,
       validation_json = EXCLUDED.validation_json,
       applied_at = COALESCE(EXCLUDED.applied_at, owner_acl_artifacts.applied_at)`,
    [
      input.generation,
      input.rendererVersion,
      input.mode,
      input.contentSha256,
      input.content,
      JSON.stringify(input.semantic),
      JSON.stringify(input.validation),
      input.applied ? new Date() : null,
    ],
  );
}
