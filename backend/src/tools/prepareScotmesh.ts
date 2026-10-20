import fs from 'node:fs/promises';
import path from 'node:path';
import { generatePacketShareIdentity } from '../owner/packetShareAuth.js';
import { getOwnerNodeIdsForUsername, closeOwnerAuthDb } from '../db/ownerAuth.js';

async function main(): Promise<void> {
  const [nodeIdArgument, outputFile] = process.argv.slice(2);
  const nodeId = nodeIdArgument?.trim().toUpperCase();
  if (!nodeId || !/^[0-9A-F]{64}$/.test(nodeId) || !outputFile) {
    throw new Error('Usage: prepareScotmesh NODE_ID OUTPUT_FILE');
  }
  try {
    const nodeIds = await getOwnerNodeIdsForUsername('node1');
    if (!nodeIds.includes(nodeId)) throw new Error('Node is not verified for node1');
    // Opening exclusively prevents an accidental rerun from rotating an identity.
    await fs.mkdir(path.dirname(outputFile), { recursive: true, mode: 0o700 });
    const file = await fs.open(outputFile, 'wx', 0o600);
    try {
      const identity = await generatePacketShareIdentity();
      const config = {
        destination_id: 'scotmesh',
        display_name: 'ScotMesh',
        description: 'MME; received packets and own transmitted adverts',
        broker_url: 'wss://scotmesh-mqtt.mm7roq.compute.oarc.uk:443/',
        topic_template: 'meshcore/MME/{observerId}/packets',
        allowed_owner_username: 'node1',
        allowed_node_id: nodeId,
        auth_mode: 'meshcore_jwt',
        auth_audience: 'scotmesh-mqtt.mm7roq.compute.oarc.uk',
        tx_mode: 'advert',
        broker_username: identity.username,
        broker_password_ciphertext: identity.privateKeyCiphertext,
        enabled: true,
      };
      await file.writeFile(`${JSON.stringify(config, null, 2)}\n`);
      console.log(JSON.stringify({ destination: 'scotmesh', owner: 'node1', nodeId, username: identity.username, outputFile }));
    } finally {
      await file.close();
    }
  } finally {
    await closeOwnerAuthDb();
  }
}

main().catch((error: unknown) => {
  console.error('[scotmesh] preparation failed:', error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
