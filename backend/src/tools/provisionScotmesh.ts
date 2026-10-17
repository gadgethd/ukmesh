import fs from 'node:fs/promises';
import pg from 'pg';
import { closeOwnerAuthDb, getOwnerNodeIdsForUsername } from '../db/ownerAuth.js';
import { databaseConfig } from '../platform/config/database.js';
import { packetShareCredentials } from '../owner/packetShareAuth.js';
import { destinationConfigured, type PacketShareDestinationRow } from '../owner/packetShareConfig.js';
import { hasPacketShareEncryptionKey } from '../owner/packetShareCrypto.js';

async function main(): Promise<void> {
  const [inputFile] = process.argv.slice(2);
  if (!inputFile) throw new Error('Usage: provisionScotmesh CONFIG_FILE');
  const pool = new pg.Pool({
    connectionString: process.env['DATABASE_URL'],
    max: 1,
    options: databaseConfig.schema ? `-c search_path=${databaseConfig.schema},public` : undefined,
  });
  try {
    const destination = JSON.parse(await fs.readFile(inputFile, 'utf8')) as PacketShareDestinationRow;
    if (destination.destination_id !== 'scotmesh'
      || destination.allowed_owner_username !== 'node1'
      || !destination.allowed_node_id || !/^[0-9A-F]{64}$/.test(destination.allowed_node_id)
      || destination.broker_url !== 'wss://scotmesh-mqtt.mm7roq.compute.oarc.uk:443/'
      || destination.topic_template !== 'meshcore/MME/{observerId}/packets'
      || destination.auth_mode !== 'meshcore_jwt'
      || destination.auth_audience !== 'scotmesh-mqtt.mm7roq.compute.oarc.uk'
      || destination.tx_mode !== 'advert'
      || !destinationConfigured(destination, hasPacketShareEncryptionKey())) {
      throw new Error('Invalid node1 ScotMesh configuration');
    }
    if (!(await getOwnerNodeIdsForUsername('node1')).includes(destination.allowed_node_id)) {
      throw new Error('Observer is no longer verified for node1');
    }
    // Decrypt and validate the persisted identity before installing credentials.
    await packetShareCredentials(destination);
    await pool.query(
      `INSERT INTO owner_packet_share_destinations (
         destination_id, display_name, description, broker_url, topic_template,
         broker_username, broker_password_ciphertext, allowed_owner_username,
         allowed_node_id, auth_mode, auth_audience, tx_mode, enabled
       ) VALUES ('scotmesh', 'ScotMesh', $1, $2, $3, $4, $5, 'node1', $6, 'meshcore_jwt', $7, 'advert', TRUE)
       ON CONFLICT (destination_id) DO UPDATE SET
         display_name = EXCLUDED.display_name, description = EXCLUDED.description,
         broker_url = EXCLUDED.broker_url, topic_template = EXCLUDED.topic_template,
         broker_username = EXCLUDED.broker_username, broker_password_ciphertext = EXCLUDED.broker_password_ciphertext,
         allowed_owner_username = EXCLUDED.allowed_owner_username, allowed_node_id = EXCLUDED.allowed_node_id,
         auth_mode = EXCLUDED.auth_mode, auth_audience = EXCLUDED.auth_audience,
         tx_mode = EXCLUDED.tx_mode, enabled = TRUE, updated_at = NOW()`,
      [destination.description, destination.broker_url, destination.topic_template,
        destination.broker_username, destination.broker_password_ciphertext,
        destination.allowed_node_id, destination.auth_audience],
    );
    console.log(JSON.stringify({ destination: 'scotmesh', owner: 'node1', nodeId: destination.allowed_node_id, iata: 'MME' }));
  } finally {
    await Promise.all([pool.end(), closeOwnerAuthDb()]);
  }
}

main().catch((error: unknown) => {
  console.error('[scotmesh] provisioning failed:', error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
