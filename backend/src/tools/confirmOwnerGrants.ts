import {
  closeOwnerAuthDb,
  getAppliedOwnerAclArtifact,
  getOwnerAclReadiness,
  getOwnerNodeIdsForUsername,
  provisionConfirmedOwnerGrants,
} from '../owner/ownerAuth.js';
import { validAppliedOwnerAclArtifact } from '../owner/ownerAclArtifact.js';
import { parseOwnerGrantConfig } from '../owner/ownerGrantConfig.js';
import { isOwnerAclGenerationSettled } from '../owner/ownerInventoryReadiness.js';
import { OWNER_ACL_RENDERER_VERSION, readAclFile } from '../mqtt/aclManager.js';

async function main(): Promise<void> {
  const [action, mode, username, actor, ...confirmedKeys] = process.argv.slice(2);
  if ((action !== 'inspect' && action !== 'record' && action !== 'verify')
    || (mode !== 'create' && mode !== 'watch' && mode !== 'link')
    || !username || !/^[A-Za-z0-9_.@-]{1,128}$/.test(username)
    || !actor || !/^[A-Za-z0-9_.@:-]{1,160}$/.test(actor)
    || (action === 'inspect' ? confirmedKeys.length !== 0 : confirmedKeys.length === 0)
    || confirmedKeys.some((key) => !/^[0-9A-F]{64}$/.test(key))) {
    throw new Error('Usage: confirmOwnerGrants inspect|record|verify create|watch|link USER ACTOR [CONFIRMED_NODE_KEY...]');
  }
  if (action !== 'inspect' && String(process.env['OWNER_ACL_MODE'] ?? 'apply').trim().toLowerCase() !== 'apply') {
    throw new Error('OWNER_PROVISIONING_REQUIRES_APPLIED_ACL');
  }

  if (action === 'verify') {
    const activeKeys = await getOwnerNodeIdsForUsername(username);
    if (confirmedKeys.some((key) => !activeKeys.includes(key))) throw new Error('OWNER_GRANT_IS_NOT_ACTIVE');
    const state = await getOwnerAclReadiness();
    const generation = state.appliedGeneration;
    if (generation && isOwnerAclGenerationSettled(state)) {
      const artifact = await getAppliedOwnerAclArtifact(generation);
      if (artifact && artifact.rendererVersion === OWNER_ACL_RENDERER_VERSION
        && validAppliedOwnerAclArtifact(artifact, generation)
        && artifact.content === readAclFile()) return;
    }
    process.exitCode = 75;
    return;
  }

  const configured = parseOwnerGrantConfig(process.env['OWNER_MQTT_USERNAME_MAP'] ?? '')
    .filter((grant) => grant.mqttUsername === username);
  if (mode === 'create' && configured.length > 0) throw new Error('OWNER_ACCOUNT_ALREADY_CONFIGURED');
  const activeKeys = await provisionConfirmedOwnerGrants({ username, mode,
    ...(action === 'record' && { approval: { nodeIds: confirmedKeys, actor,
      configuredKeys: configured.map((grant) => grant.nodeId) } }),
  });
  if (action === 'inspect') {
    for (const key of activeKeys) console.log(key);
  }
}

try {
  await main();
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
} finally {
  await closeOwnerAuthDb();
}
