import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import Docker from 'dockerode';

export const OWNER_ACL_RENDERER_VERSION = 'meshcore-owner-acl/v1';
const MANAGED_BEGIN = '# BEGIN MESHCORE OWNER ACL';
const MANAGED_END = '# END MESHCORE OWNER ACL';
const NODE_ID_RE = /^[0-9A-F]{64}$/;

export type OwnerAclGrant = {
  mqttUsername: string;
  nodeIds: string[];
};

export type AclDirectiveClass =
  | 'canonical-owner-write'
  | 'wildcard'
  | 'pattern'
  | 'other'
  | 'malformed';

export type AclUserStanza = {
  username: string;
  rawLines: string[];
  directives: Array<{ line: string; classification: AclDirectiveClass }>;
};

export type ParsedAcl = {
  preamble: string[];
  stanzas: AclUserStanza[];
  duplicateUsers: string[];
  managedSectionFound: boolean;
  errors: string[];
};

export type OwnerAclRenderResult = {
  generation: string;
  contentSha256: string;
  content: string;
  semantic: OwnerAclGrant[];
  validation: {
    ok: boolean;
    ambiguousUsers: string[];
    duplicateUsers: string[];
    emptyManagedUsers: string[];
    malformedLines: string[];
    replacedUsers: string[];
  };
};

function normalizeUsername(value: string): string {
  const username = value.trim();
  if (!/^[A-Za-z0-9_.@-]{1,128}$/.test(username)) throw new Error(`INVALID_MQTT_USERNAME:${username}`);
  return username;
}

function normalizeNodeIds(nodeIds: string[]): string[] {
  return Array.from(new Set(nodeIds.map((value) => value.trim().toUpperCase()).filter((value) => NODE_ID_RE.test(value)))).sort();
}

function stripManagedSection(content: string): { content: string; found: boolean; errors: string[] } {
  const lines = content.replace(/\r\n/g, '\n').split('\n');
  const output: string[] = [];
  const errors: string[] = [];
  let inManaged = false;
  let found = false;
  for (const line of lines) {
    if (line.trim().startsWith(MANAGED_BEGIN)) {
      if (inManaged) errors.push('nested managed ACL section');
      inManaged = true;
      found = true;
      continue;
    }
    if (line.trim() === MANAGED_END) {
      if (!inManaged) errors.push('managed ACL end marker without begin marker');
      inManaged = false;
      continue;
    }
    if (!inManaged) output.push(line);
  }
  if (inManaged) errors.push('managed ACL section is missing its end marker');
  return { content: output.join('\n'), found, errors };
}

function classifyDirective(line: string): AclDirectiveClass {
  const trimmed = line.trim();
  if (/^topic\s+write\s+meshcore\/\+\/[0-9A-Fa-f]{64}\/(?:packets|status)$/.test(trimmed)) {
    return 'canonical-owner-write';
  }
  if (/^pattern\s+/i.test(trimmed)) return 'pattern';
  if (/^(?:topic|pattern)\s+/i.test(trimmed) && /[# +]/.test(trimmed.replace(/^topic\s+\w+\s+/i, ''))) {
    return 'wildcard';
  }
  if (/^topic\s+(?:read|write|readwrite|deny)\s+\S+$/.test(trimmed)) return 'other';
  return 'malformed';
}

export function parseAcl(content: string): ParsedAcl {
  const managed = stripManagedSection(content);
  const lines = content.replace(/\r\n/g, '\n').split('\n');
  const preamble: string[] = [];
  const stanzas: AclUserStanza[] = [];
  let current: AclUserStanza | null = null;

  for (const line of lines) {
    const userMatch = line.trim().match(/^user\s+(\S+)\s*$/);
    if (userMatch) {
      current = { username: userMatch[1]!, rawLines: [line], directives: [] };
      stanzas.push(current);
      continue;
    }
    if (!current) {
      preamble.push(line);
      continue;
    }
    current.rawLines.push(line);
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#')) {
      current.directives.push({ line: trimmed, classification: classifyDirective(trimmed) });
    }
  }

  const counts = new Map<string, number>();
  for (const stanza of stanzas) counts.set(stanza.username, (counts.get(stanza.username) ?? 0) + 1);
  return {
    preamble,
    stanzas,
    duplicateUsers: [...counts].filter(([, count]) => count > 1).map(([username]) => username).sort(),
    managedSectionFound: managed.found,
    errors: managed.errors,
  };
}

function buildManagedSection(grants: OwnerAclGrant[], generation: string): string[] {
  const lines = [`${MANAGED_BEGIN} v1 generation=${generation}`];
  for (const grant of grants) {
    lines.push(`user ${grant.mqttUsername}`);
    for (const nodeId of grant.nodeIds) {
      lines.push(`topic write meshcore/+/${nodeId}/packets`);
      lines.push(`topic write meshcore/+/${nodeId}/status`);
    }
    lines.push('');
  }
  while (lines.at(-1) === '') lines.pop();
  lines.push(MANAGED_END);
  return lines;
}

export function renderOwnerAcl(
  existingContent: string,
  grants: OwnerAclGrant[],
  unmanagedUsers: Iterable<string>,
  allowedEmptyUsers: Iterable<string> = [],
): OwnerAclRenderResult {
  const unmanaged = new Set([...unmanagedUsers].map((value) => normalizeUsername(value)));
  const normalized = grants.map((grant) => ({
    mqttUsername: normalizeUsername(grant.mqttUsername),
    nodeIds: normalizeNodeIds(grant.nodeIds),
  }))
    .filter((grant) => !unmanaged.has(grant.mqttUsername))
    .sort((a, b) => a.mqttUsername.localeCompare(b.mqttUsername));
  const usernames = new Set(normalized.map((grant) => grant.mqttUsername));
  if (usernames.size !== normalized.length) throw new Error('DUPLICATE_OWNER_GRANT_USERNAME');

  const generation = createHash('sha256')
    .update(JSON.stringify({ renderer: OWNER_ACL_RENDERER_VERSION, grants: normalized }))
    .digest('hex');
  const stripped = stripManagedSection(existingContent);
  const parsed = parseAcl(stripped.content);
  parsed.managedSectionFound = stripped.found;
  parsed.errors.push(...stripped.errors);
  const allowedEmpty = new Set([...allowedEmptyUsers].map((value) => normalizeUsername(value)));
  const ambiguousUsers = Array.from(new Set(
    parsed.stanzas
      .filter((stanza) => !usernames.has(stanza.username) && !unmanaged.has(stanza.username))
      .map((stanza) => stanza.username),
  )).sort();
  const malformedLines = parsed.stanzas.flatMap((stanza) =>
    stanza.directives
      .filter((directive) => directive.classification === 'malformed')
      .map((directive) => `${stanza.username}: ${directive.line}`));
  const emptyManagedUsers = normalized
    .filter((grant) => grant.nodeIds.length === 0 && !allowedEmpty.has(grant.mqttUsername))
    .map((grant) => grant.mqttUsername);

  const retained = parsed.stanzas.filter((stanza) => unmanaged.has(stanza.username));
  const output = [...parsed.preamble];
  while (output.length > 0 && output.at(-1)?.trim() === '') output.pop();
  for (const stanza of retained) {
    if (output.length > 0) output.push('');
    output.push(...stanza.rawLines);
    while (output.at(-1)?.trim() === '') output.pop();
  }
  if (output.length > 0) output.push('');
  output.push(...buildManagedSection(normalized, generation));
  const content = `${output.join('\n').trimEnd()}\n`;
  const contentSha256 = createHash('sha256').update(content).digest('hex');

  return {
    generation,
    contentSha256,
    content,
    semantic: normalized,
    validation: {
      ok: parsed.errors.length === 0
        && parsed.duplicateUsers.length === 0
        && ambiguousUsers.length === 0
        && emptyManagedUsers.length === 0
        && malformedLines.length === 0,
      ambiguousUsers,
      duplicateUsers: parsed.duplicateUsers,
      emptyManagedUsers,
      malformedLines: [...parsed.errors, ...malformedLines],
      replacedUsers: parsed.stanzas.filter((stanza) => usernames.has(stanza.username)).map((stanza) => stanza.username).sort(),
    },
  };
}

export function validateRenderedOwnerAcl(content: string, expected: OwnerAclRenderResult): void {
  const sha256 = createHash('sha256').update(content).digest('hex');
  if (sha256 !== expected.contentSha256) throw new Error('OWNER_ACL_READBACK_HASH_MISMATCH');
  const marker = `${MANAGED_BEGIN} v1 generation=${expected.generation}`;
  if (!content.includes(marker) || !content.includes(MANAGED_END)) {
    throw new Error('OWNER_ACL_MANAGED_MARKERS_MISSING');
  }
  for (const grant of expected.semantic) {
    for (const nodeId of grant.nodeIds) {
      if (!content.includes(`topic write meshcore/+/${nodeId}/packets`)
        || !content.includes(`topic write meshcore/+/${nodeId}/status`)) {
        throw new Error(`OWNER_ACL_SEMANTIC_MISMATCH:${grant.mqttUsername}:${nodeId}`);
      }
    }
  }
}

export function getNodeIdsForUserInAcl(content: string, mqttUsername: string): string[] {
  const nodeIds: string[] = [];
  for (const stanza of parseAcl(content).stanzas.filter((candidate) => candidate.username === mqttUsername)) {
    for (const directive of stanza.directives) {
      const match = directive.line.match(/^topic\s+write\s+meshcore\/\+\/([0-9A-Fa-f]{64})\/(?:packets|status)$/);
      if (match) nodeIds.push(match[1]!.toUpperCase());
    }
  }
  return normalizeNodeIds(nodeIds);
}

export function userExistsInAclContent(content: string, mqttUsername: string): boolean {
  return parseAcl(content).stanzas.some((stanza) => stanza.username === mqttUsername);
}

export function updateUserAclContent(content: string, mqttUsername: string, nodeIds: string[]): string {
  const parsed = parseAcl(content);
  const grants = parsed.stanzas
    .filter((stanza) => stanza.username !== mqttUsername)
    .map((stanza) => ({ mqttUsername: stanza.username, nodeIds: getNodeIdsForUserInAcl(content, stanza.username) }));
  grants.push({ mqttUsername, nodeIds });
  return renderOwnerAcl('', grants, [], grants.filter((grant) => grant.nodeIds.length === 0).map((grant) => grant.mqttUsername)).content;
}

export function readAclFile(aclPath = process.env['MOSQUITTO_ACL_PATH'] ?? '/mosquitto/config/acl'): string {
  return fs.readFileSync(aclPath, 'utf8');
}

export function writeAclAtomically(
  content: string,
  aclPath = process.env['MOSQUITTO_ACL_PATH'] ?? '/mosquitto/config/acl',
): void {
  const directory = path.dirname(aclPath);
  const temporaryPath = path.join(directory, `.${path.basename(aclPath)}.${process.pid}.tmp`);
  const lastKnownGoodPath = `${aclPath}.lkg`;
  const previous = fs.readFileSync(aclPath, 'utf8');
  const stat = fs.statSync(aclPath);
  fs.writeFileSync(lastKnownGoodPath, previous, { encoding: 'utf8', mode: stat.mode & 0o777 });
  const descriptor = fs.openSync(temporaryPath, 'wx', stat.mode & 0o777);
  try {
    fs.fchownSync(descriptor, stat.uid, stat.gid);
    fs.fchmodSync(descriptor, stat.mode & 0o777);
    fs.writeFileSync(descriptor, content, 'utf8');
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  fs.renameSync(temporaryPath, aclPath);
  const directoryDescriptor = fs.openSync(directory, 'r');
  try {
    fs.fsyncSync(directoryDescriptor);
  } finally {
    fs.closeSync(directoryDescriptor);
  }
}

export async function reloadMosquitto(): Promise<void> {
  const socketPath = process.env['DOCKER_SOCKET'] ?? '/var/run/docker.sock';
  if (!fs.existsSync(socketPath)) throw new Error(`DOCKER_SOCKET_NOT_FOUND:${socketPath}`);
  const docker = new Docker({ socketPath });
  const containers = await docker.listContainers();
  const label = process.env['MOSQUITTO_CONTAINER_NAME'] ?? 'mosquitto';
  const matches = containers.filter((container) => container.Names.some((name) => name.includes(label)));
  if (matches.length !== 1) throw new Error(`MOSQUITTO_CONTAINER_MATCH_COUNT:${matches.length}`);
  await docker.getContainer(matches[0]!.Id).kill({ signal: 'SIGHUP' });
}
