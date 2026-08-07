import {
  closeOwnerAuthDb,
  getOwnerAuthorizationInventory,
  getOwnerAuthorizationSnapshot,
  initOwnerAuthDb,
} from '../db/ownerAuth.js';
import { parseAcl, readAclFile, renderOwnerAcl } from '../mqtt/aclManager.js';
import { parseOwnerGrantConfig } from '../owner/ownerGrantConfig.js';

async function main(): Promise<void> {
  await initOwnerAuthDb();
  const [database, snapshot] = await Promise.all([
    getOwnerAuthorizationInventory(),
    getOwnerAuthorizationSnapshot(),
  ]);
  const aclContent = readAclFile();
  const parsedAcl = parseAcl(aclContent);
  const configured = parseOwnerGrantConfig(String(process.env['OWNER_MQTT_USERNAME_MAP'] ?? ''));
  const unmanagedUsers = String(process.env['OWNER_ACL_UNMANAGED_USERS'] ?? 'backend,test,test2')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  const rendered = renderOwnerAcl(
    aclContent,
    snapshot.map((account) => ({
      mqttUsername: account.mqttUsername,
      nodeIds: account.nodeIds,
    })),
    unmanagedUsers,
    String(process.env['OWNER_ACL_ALLOW_EMPTY_USERS'] ?? '').split(',').map((value) => value.trim()).filter(Boolean),
  );
  const configKeys = new Set(configured.map((grant) => `${grant.mqttUsername}\0${grant.nodeId}`));
  const legacyAclGrants = parsedAcl.stanzas.flatMap((stanza) =>
    stanza.directives
      .map((directive) => directive.line.match(
        /^topic\s+write\s+meshcore\/\+\/([0-9A-Fa-f]{64})\/(?:packets|status|neighbors|neighbours)$/,
      )?.[1]?.toUpperCase())
      .filter((nodeId): nodeId is string => Boolean(nodeId))
      .map((nodeId) => ({ mqttUsername: stanza.username, nodeId })))
    .filter((grant, index, all) =>
      all.findIndex((candidate) =>
        candidate.mqttUsername === grant.mqttUsername && candidate.nodeId === grant.nodeId) === index);
  const proposedBackfill = legacyAclGrants.filter((grant) =>
    !configKeys.has(`${grant.mqttUsername}\0${grant.nodeId}`)
    && !unmanagedUsers.includes(grant.mqttUsername));

  const report = {
    generatedAt: new Date().toISOString(),
    cutoverReady: rendered.validation.ok
      && rendered.semantic.some((account) => account.nodeIds.length > 0),
    configuredGrants: configured,
    verifiedSnapshot: snapshot,
    database,
    acl: {
      users: parsedAcl.stanzas.map((stanza) => ({
        mqttUsername: stanza.username,
        directives: stanza.directives,
      })),
      duplicateUsers: parsedAcl.duplicateUsers,
      parserErrors: parsedAcl.errors,
      validation: rendered.validation,
      desiredGeneration: rendered.generation,
    },
    proposedBackfill,
    warning: 'proposedBackfill is inventory only; review each owner/node pair before adding it to operator configuration',
  };
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  if (process.argv.includes('--require-complete') && !report.cutoverReady) process.exitCode = 2;
}

main()
  .catch((error: Error) => {
    console.error('[owner-auth-inventory]', error.message);
    process.exitCode = 1;
  })
  .finally(() => closeOwnerAuthDb().catch(() => undefined));
