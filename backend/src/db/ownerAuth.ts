import pg from 'pg';
import fs from 'node:fs';
import { randomUUID } from 'node:crypto';
import { resolveDbAssetPath } from './assets.js';

const { Pool } = pg;
const OWNER_DB_NAME = process.env['OWNER_POSTGRES_DB'] ?? 'meshcore_owner_auth';
const ownerDatabaseApplicationName = String(process.env['OWNER_DATABASE_APPLICATION_NAME'] ?? 'meshcore-owner-auth').trim() || 'meshcore-owner-auth';
const ownerAdminDatabaseApplicationName = String(process.env['OWNER_DATABASE_ADMIN_APPLICATION_NAME'] ?? 'meshcore-owner-auth-admin').trim() || 'meshcore-owner-auth-admin';
const ownerDatabaseStatementTimeoutMs = Number(process.env['OWNER_DATABASE_STATEMENT_TIMEOUT_MS'] ?? 30_000);

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
       AND oan.verification_method IS NOT NULL
       AND oan.revoked_at IS NULL
     ORDER BY oan.node_id ASC`,
    [normalized],
  );
  return res.rows.map((row) => row.node_id);
}

export async function getOwnerAuthorizationSnapshot(): Promise<Array<{
  mqttUsername: string;
  nodeIds: string[];
}>> {
  const res = await ownerPool.query<{
    mqtt_username: string;
    node_ids: string[] | null;
  }>(
    `SELECT
       oa.mqtt_username,
       COALESCE(
         ARRAY_AGG(oan.node_id ORDER BY oan.node_id) FILTER (
           WHERE oa.is_active = TRUE
             AND oan.node_id ~ '^[0-9A-F]{64}$'
             AND oan.verified_at IS NOT NULL
             AND oan.verification_method IS NOT NULL
             AND oan.revoked_at IS NULL
         ),
         ARRAY[]::text[]
       ) AS node_ids
     FROM owner_accounts oa
     LEFT JOIN owner_account_nodes oan ON oan.mqtt_username = oa.mqtt_username
     GROUP BY oa.mqtt_username
     ORDER BY oa.mqtt_username`,
  );
  return res.rows.map((row) => ({
    mqttUsername: row.mqtt_username,
    nodeIds: row.node_ids ?? [],
  }));
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

export async function upsertVerifiedOwnerGrant(
  mqttUsername: string,
  nodeId: string,
  verificationMethod: OwnerGrantVerificationMethod,
): Promise<void> {
  const normalizedUsername = mqttUsername.trim();
  const normalizedNodeId = nodeId.trim().toUpperCase();
  if (!normalizedUsername || !/^[0-9A-F]{64}$/.test(normalizedNodeId)) {
    throw new Error('INVALID_OWNER_GRANT');
  }
  await ensureOwnerAccount(normalizedUsername);
  await ownerPool.query(
    `INSERT INTO owner_account_nodes (
       mqtt_username, node_id, verification_method, verified_at, grant_id, revoked_at
     )
     VALUES ($1, $2, $3, NOW(), $4, NULL)
     ON CONFLICT (mqtt_username, node_id)
     DO UPDATE SET
       verification_method = EXCLUDED.verification_method,
       verified_at = NOW(),
       grant_id = EXCLUDED.grant_id,
       revoked_at = NULL`,
    [normalizedUsername, normalizedNodeId, verificationMethod, randomUUID()],
  );
}

export async function syncOperatorConfiguredOwnerGrants(
  grants: Array<{ mqttUsername: string; nodeId: string }>,
): Promise<void> {
  const desired = grants
    .map((grant) => ({
      mqtt_username: grant.mqttUsername.trim(),
      node_id: grant.nodeId.trim().toUpperCase(),
      grant_id: randomUUID(),
    }))
    .filter((grant) => grant.mqtt_username && /^[0-9A-F]{64}$/.test(grant.node_id));
  const client = await ownerPool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO owner_accounts (mqtt_username, is_active, updated_at)
       SELECT DISTINCT row.mqtt_username, TRUE, NOW()
       FROM jsonb_to_recordset($1::jsonb) AS row(mqtt_username text, node_id text, grant_id text)
       ON CONFLICT (mqtt_username)
       DO UPDATE SET is_active = TRUE, updated_at = NOW()`,
      [JSON.stringify(desired)],
    );
    await client.query(
      `INSERT INTO owner_account_nodes (
         mqtt_username, node_id, verification_method, verified_at, grant_id, revoked_at
       )
       SELECT row.mqtt_username, row.node_id, 'operator-config', NOW(), row.grant_id, NULL
       FROM jsonb_to_recordset($1::jsonb) AS row(mqtt_username text, node_id text, grant_id text)
       ON CONFLICT (mqtt_username, node_id)
       DO UPDATE SET
         verification_method = CASE
           WHEN owner_account_nodes.verification_method = 'operator-database'
             AND owner_account_nodes.revoked_at IS NULL
           THEN owner_account_nodes.verification_method
           ELSE 'operator-config'
         END,
         verified_at = CASE
           WHEN owner_account_nodes.verification_method = 'operator-database'
             AND owner_account_nodes.revoked_at IS NULL
           THEN owner_account_nodes.verified_at
           ELSE NOW()
         END,
         grant_id = CASE
           WHEN owner_account_nodes.verification_method = 'operator-database'
             AND owner_account_nodes.revoked_at IS NULL
           THEN owner_account_nodes.grant_id
           ELSE EXCLUDED.grant_id
         END,
         revoked_at = NULL`,
      [JSON.stringify(desired)],
    );
    await client.query(
      `UPDATE owner_account_nodes existing
       SET revoked_at = NOW()
       WHERE existing.verification_method = 'operator-config'
         AND existing.revoked_at IS NULL
         AND NOT EXISTS (
           SELECT 1
           FROM jsonb_to_recordset($1::jsonb) AS row(mqtt_username text, node_id text, grant_id text)
           WHERE row.mqtt_username = existing.mqtt_username
             AND row.node_id = existing.node_id
         )`,
      [JSON.stringify(desired)],
    );
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

export async function recordUntrustedMqttNodeObservation(mqttUsername: string, nodeId: string): Promise<void> {
  const normalizedUsername = mqttUsername.trim();
  const normalizedNodeId = nodeId.trim().toUpperCase();
  if (!normalizedUsername || !/^[0-9A-F]{64}$/.test(normalizedNodeId)) return;
  await ownerPool.query(
    `INSERT INTO mqtt_node_logins (mqtt_username, node_id, last_connected_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (mqtt_username, node_id) DO UPDATE SET last_connected_at = NOW()`,
    [normalizedUsername, normalizedNodeId],
  );
}

export async function revokeOwnerGrant(mqttUsername: string, nodeId: string): Promise<void> {
  const normalizedUsername = mqttUsername.trim();
  const normalizedNodeId = nodeId.trim().toUpperCase();
  if (!normalizedUsername || !/^[0-9A-F]{64}$/.test(normalizedNodeId)) return;
  await ownerPool.query(
    `UPDATE owner_account_nodes
        SET revoked_at = NOW()
      WHERE mqtt_username = $1
        AND node_id = $2`,
    [normalizedUsername, normalizedNodeId],
  );
}
