import {
  closeDb,
  query,
} from '../db/index.js';
import {
  closeOwnerAuthDb,
  getOwnerAclReadiness,
  getOwnerAuthorizationInventory,
  getOwnerAuthorizationSnapshot,
} from '../db/ownerAuth.js';
import { parseAcl, readAclFile, renderOwnerAcl } from '../mqtt/aclManager.js';
import { parseOwnerGrantConfig } from '../owner/ownerGrantConfig.js';
import { getPublicFeedConfig } from '../owner/publicFeedConfig.js';
import {
  getActivePublicFeedReaderUsernames,
} from '../owner/publicFeedRepository.js';
import {
  buildOwnerInventoryBaseline,
  loadOwnerInventoryBaseline,
  validateOwnerInventoryBaseline,
  writeOwnerInventoryBaseline,
} from '../owner/ownerInventoryBaseline.js';

function argValue(name: string): string | undefined {
  const prefix = `${name}=`;
  return process.argv.find((arg) => arg.startsWith(prefix))?.slice(prefix.length);
}

async function main(): Promise<void> {
  const [database, snapshot, aclState] = await Promise.all([
    getOwnerAuthorizationInventory(),
    getOwnerAuthorizationSnapshot(),
    getOwnerAclReadiness(),
  ]);
  const aclContent = readAclFile();
  const parsedAcl = parseAcl(aclContent);
  const configured = parseOwnerGrantConfig(String(process.env['OWNER_MQTT_USERNAME_MAP'] ?? ''));
  const publicFeedConfig = getPublicFeedConfig();
  const publicFeedReaders = publicFeedConfig.configured
    ? await getActivePublicFeedReaderUsernames(query)
    : new Set<string>();
  const explicitPublicFeedReaders = String(process.env['OWNER_ACL_PUBLIC_FEED_READ_USERS'] ?? '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  const explicitPublicFeedReaderSet = new Set(explicitPublicFeedReaders);
  const grants = snapshot.map((account) => {
    const publicFeedRead = explicitPublicFeedReaderSet.has(account.mqttUsername)
      || (account.isActive && publicFeedReaders.has(account.mqttUsername));
    return {
      mqttUsername: account.mqttUsername,
      nodeIds: account.isActive ? account.nodeIds : [],
      ...(publicFeedRead ? { publicFeedRead: true } : {}),
    };
  });
  const snapshotUsernames = new Set(grants.map((grant) => grant.mqttUsername));
  for (const mqttUsername of explicitPublicFeedReaders) {
    if (snapshotUsernames.has(mqttUsername)) continue;
    grants.push({
      mqttUsername,
      nodeIds: [],
      publicFeedRead: true,
    });
  }
  const unmanagedUsers = String(process.env['OWNER_ACL_UNMANAGED_USERS'] ?? 'backend,test,test2')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  const rendered = renderOwnerAcl(
    aclContent,
    grants,
    unmanagedUsers,
    [
      ...String(process.env['OWNER_ACL_ALLOW_EMPTY_USERS'] ?? '')
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean),
      ...explicitPublicFeedReaders,
    ],
    [String(process.env['MQTT_USERNAME'] ?? 'backend').trim() || 'backend'],
    String(process.env['OWNER_ACL_SYSTEM_READWRITE_TOPICS'] ?? 'public/#')
      .split(',')
      .map((value) => value.trim())
      .filter(Boolean),
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
  const inventoryBaseline = buildOwnerInventoryBaseline({
    accounts: database.accounts,
    configuredGrants: configured,
    aclContent,
    aclState,
  });
  const exportPath = argValue('--export');
  if (exportPath) writeOwnerInventoryBaseline(exportPath, inventoryBaseline);
  const baselinePath = argValue('--baseline');
  const baselineValidation = baselinePath
    ? validateOwnerInventoryBaseline(
        loadOwnerInventoryBaseline(baselinePath),
        inventoryBaseline,
      )
    : null;

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
    inventoryBaseline,
    baselineValidation,
    warning: 'proposedBackfill is inventory only; review each owner/node pair before adding it to operator configuration',
  };
  if (process.argv.includes('--show-details')) {
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  } else {
    process.stdout.write(`${JSON.stringify({
      generatedAt: inventoryBaseline.generatedAt,
      cutoverReady: report.cutoverReady,
      counts: inventoryBaseline.counts,
      configuredGeneration: inventoryBaseline.configuredGeneration,
      aclState: inventoryBaseline.aclState,
      contentSha256: inventoryBaseline.contentSha256,
      legacyDisposition: inventoryBaseline.legacyDisposition,
      exportPath: exportPath ?? null,
      baselinePath: baselinePath ?? null,
      baselineValidation,
    }, null, 2)}\n`);
  }
  if (process.argv.includes('--require-complete')
    && (!report.cutoverReady || baselineValidation?.ok === false)) process.exitCode = 2;
}

main()
  .catch((error: Error) => {
    console.error('[owner-auth-inventory]', error.message);
    process.exitCode = 1;
  })
.finally(async () => {
  await closeDb().catch(() => undefined);
  await closeOwnerAuthDb().catch(() => undefined);
});
