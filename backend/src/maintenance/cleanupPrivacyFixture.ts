import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

// Shared isolated-test fixture for PostgreSQL-WASM and native PostgreSQL.
export async function installPrivacyTriggers(db: { exec(sql: string): Promise<unknown> }) {
  await db.exec(`
    ALTER TABLE packets
      ADD COLUMN rx_node_id TEXT, ADD COLUMN network TEXT DEFAULT 'ukmesh',
      ADD COLUMN path_hashes TEXT[], ADD COLUMN path_hash_size_bytes INTEGER,
      ADD COLUMN is_private BOOLEAN DEFAULT FALSE, ADD COLUMN visibility_ok BOOLEAN DEFAULT TRUE;
    CREATE TABLE packet_paths (
      src_node_id TEXT, rx_node_id TEXT, network TEXT DEFAULT 'ukmesh',
      path_hashes TEXT[], path_hash_size_bytes INTEGER,
      is_private BOOLEAN DEFAULT FALSE, visibility_ok BOOLEAN DEFAULT TRUE
    );
  `);
  // Install the current function bodies and relevant trigger chain into this
  // disposable fixture. Never execute a project migration or connect to a server.
  for (const [file, names] of [
    ['042_packet_visibility_fence.sql', ['lock_packet_visibility_for_node_privacy_change', 'bump_public_visibility_generation', 'bump_visibility_for_identity_table']],
    ['049_guard_packet_privacy_rewrite.sql', ['sync_private_node_prefixes']],
    ['051_reset_readiness.sql', ['meshcore_path_matches_private', 'meshcore_path_is_valid', 'classify_packet_path_privacy', 'rematerialize_packet_path_privacy']],
  ] as const) {
    const source = await readFile(new URL(`../db/migrations/${file}`, import.meta.url), 'utf8');
    for (const name of names) {
      const definition = source.match(new RegExp(`CREATE OR REPLACE FUNCTION ${name}\\([\\s\\S]*?^\\$\\$;`, 'm'))?.[0];
      assert.ok(definition, `the production ${name} definition must be present`);
      await db.exec(definition);
    }
  }
  await db.exec(`
    CREATE TRIGGER nodes_packet_visibility_serialization BEFORE INSERT OR UPDATE OR DELETE ON nodes
      FOR EACH ROW EXECUTE FUNCTION lock_packet_visibility_for_node_privacy_change();
    CREATE TRIGGER nodes_private_prefix_materialization AFTER INSERT OR UPDATE OR DELETE ON nodes
      FOR EACH ROW EXECUTE FUNCTION sync_private_node_prefixes();
    CREATE TRIGGER nodes_public_visibility_generation AFTER INSERT OR UPDATE OR DELETE ON nodes
      FOR EACH ROW EXECUTE FUNCTION bump_public_visibility_generation();
    CREATE TRIGGER nodes_private_zz_packet_path_materialization AFTER INSERT OR UPDATE OR DELETE ON nodes
      FOR EACH ROW EXECUTE FUNCTION rematerialize_packet_path_privacy();
    CREATE TRIGGER private_node_prefixes_visibility_generation AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON private_node_prefixes
      FOR EACH STATEMENT EXECUTE FUNCTION bump_visibility_for_identity_table();
    CREATE TRIGGER packet_paths_classify_privacy BEFORE INSERT OR UPDATE OF rx_node_id, src_node_id, path_hashes, path_hash_size_bytes, network ON packet_paths
      FOR EACH ROW EXECUTE FUNCTION classify_packet_path_privacy();
  `);
}

