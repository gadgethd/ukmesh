import pg from 'pg';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const { Pool } = pg;
const OWNER_DB_NAME = process.env['OWNER_POSTGRES_DB'] ?? 'meshcore_owner_auth';
const ownerDatabaseApplicationName = String(process.env['OWNER_DATABASE_APPLICATION_NAME'] ?? 'meshcore-owner-auth').trim() || 'meshcore-owner-auth';
const ownerAdminDatabaseApplicationName = String(process.env['OWNER_DATABASE_ADMIN_APPLICATION_NAME'] ?? 'meshcore-owner-auth-admin').trim() || 'meshcore-owner-auth-admin';

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
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const schemaPath = path.join(__dirname, 'owner-auth.sql');
  const sql = fs.readFileSync(schemaPath, 'utf8');
  await ownerPool.query(sql);
  console.log('[owner-auth] schema initialised');
}

export async function getOwnerNodeIdsForUsername(mqttUsername: string): Promise<string[]> {
  const normalized = mqttUsername.trim();
  if (!normalized) return [];
  const res = await ownerPool.query<{ node_id: string }>(
    `SELECT DISTINCT LOWER(oan.node_id) AS node_id
     FROM owner_account_nodes oan
     JOIN owner_accounts oa ON oa.mqtt_username = oan.mqtt_username
     WHERE oa.is_active = true
       AND oan.mqtt_username = $1
       AND oan.node_id ~ '^[0-9a-fA-F]{64}$'
     ORDER BY LOWER(oan.node_id) ASC`,
    [normalized],
  );
  return res.rows.map((row) => row.node_id);
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

export async function addOwnerNodeForUsername(mqttUsername: string, nodeId: string): Promise<void> {
  const normalizedUsername = mqttUsername.trim();
  const normalizedNodeId = nodeId.trim().toLowerCase();
  if (!normalizedUsername || !/^[0-9a-f]{64}$/.test(normalizedNodeId)) return;
  await ensureOwnerAccount(normalizedUsername);
  await ownerPool.query(
    `INSERT INTO owner_account_nodes (mqtt_username, node_id)
     VALUES ($1, $2)
     ON CONFLICT (mqtt_username, node_id) DO NOTHING`,
    [normalizedUsername, normalizedNodeId],
  );
}

export async function getMappedOwnerNodeIds(): Promise<string[]> {
  const res = await ownerPool.query<{ node_id: string }>(
    `SELECT DISTINCT LOWER(node_id) AS node_id
     FROM owner_account_nodes
     WHERE node_id ~ '^[0-9a-fA-F]{64}$'`,
  );
  return res.rows.map((row) => row.node_id);
}
