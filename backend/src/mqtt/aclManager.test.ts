import assert from 'node:assert/strict';
import test from 'node:test';
import {
  getNodeIdsForUserInAcl,
  parseAcl,
  renderOwnerAcl,
  updateUserAclContent,
  userExistsInAclContent,
  validateRenderedOwnerAcl,
} from './aclManager.js';

const NODE_ID = 'A1'.repeat(32);
const OTHER_NODE_ID = 'B2'.repeat(32);

test('upgrades an empty keyless user block with exact per-node publish rules', () => {
  const initial = [
    'user existing',
    `topic write meshcore/+/${NODE_ID}/packets`,
    `topic write meshcore/+/${NODE_ID}/status`,
    '',
    'user keyless.user',
    '',
  ].join('\n');

  assert.equal(userExistsInAclContent(initial, 'keyless.user'), true);
  assert.deepEqual(getNodeIdsForUserInAcl(initial, 'keyless.user'), []);

  const updated = updateUserAclContent(initial, 'keyless.user', [NODE_ID]);
  assert.deepEqual(getNodeIdsForUserInAcl(updated, 'keyless.user'), [NODE_ID]);
  assert.match(updated, new RegExp(`user keyless\\.user\\ntopic write meshcore/\\+/${NODE_ID}/packets`));
});

test('matches literal usernames instead of treating punctuation as a regular expression', () => {
  const acl = 'user keylessXuser\n';
  assert.equal(userExistsInAclContent(acl, 'keyless.user'), false);
  assert.equal(userExistsInAclContent(`${acl}user keyless.user\n`, 'keyless.user'), true);
});

test('classifies wildcard and malformed directives instead of treating them as owner grants', () => {
  const parsed = parseAcl([
    'user wildcard',
    'topic write meshcore/#',
    'user malformed',
    'totally invalid',
  ].join('\n'));

  assert.equal(parsed.stanzas[0]?.directives[0]?.classification, 'wildcard');
  assert.equal(parsed.stanzas[1]?.directives[0]?.classification, 'malformed');
  assert.deepEqual(getNodeIdsForUserInAcl('user wildcard\ntopic write meshcore/#\n', 'wildcard'), []);
});

test('canonical renderer blocks ambiguous users and removes excess managed directives', () => {
  const existing = [
    'user backend',
    'topic readwrite meshcore/#',
    '',
    'user owner',
    'topic write meshcore/#',
    '',
    'user unknown',
    `topic write meshcore/+/${OTHER_NODE_ID}/packets`,
  ].join('\n');
  const rendered = renderOwnerAcl(existing, [{ mqttUsername: 'owner', nodeIds: [NODE_ID] }], ['backend']);

  assert.equal(rendered.validation.ok, false);
  assert.deepEqual(rendered.validation.ambiguousUsers, ['unknown']);
  assert.doesNotMatch(rendered.content, /user owner\ntopic write meshcore\/#/);
  assert.match(rendered.content, new RegExp(`user owner\\ntopic write meshcore/\\+/${NODE_ID}/packets`));
});

test('canonical renderer is deterministic and readback validation detects tampering', () => {
  const grants = [{ mqttUsername: 'owner', nodeIds: [NODE_ID, NODE_ID] }];
  const first = renderOwnerAcl('user backend\ntopic readwrite meshcore/#\n', grants, ['backend']);
  const second = renderOwnerAcl(first.content, grants, ['backend']);

  assert.equal(first.content, second.content);
  assert.equal(first.generation, second.generation);
  validateRenderedOwnerAcl(first.content, first);
  assert.throws(
    () => validateRenderedOwnerAcl(first.content.replace('/packets', '/other'), first),
    /OWNER_ACL_READBACK_HASH_MISMATCH/,
  );
});

test('unmanaged users are retained only once even when present in the owner snapshot', () => {
  const existing = [
    'user test',
    'topic write meshcore/#',
    '',
  ].join('\n');

  const rendered = renderOwnerAcl(
    existing,
    [{ mqttUsername: 'test', nodeIds: [] }],
    ['test'],
  );

  assert.equal(rendered.validation.ok, true);
  assert.equal((rendered.content.match(/^user test$/gm) ?? []).length, 1);
  assert.match(rendered.content, /topic write meshcore\/#/);
  assert.deepEqual(rendered.semantic, []);
});

test('cutover validation blocks empty managed accounts unless explicitly reviewed', () => {
  const blocked = renderOwnerAcl('', [{ mqttUsername: 'revoked', nodeIds: [] }], []);
  assert.equal(blocked.validation.ok, false);
  assert.deepEqual(blocked.validation.emptyManagedUsers, ['revoked']);

  const reviewed = renderOwnerAcl('', [{ mqttUsername: 'revoked', nodeIds: [] }], [], ['revoked']);
  assert.equal(reviewed.validation.ok, true);
});
