import assert from 'node:assert/strict';
import test from 'node:test';
import {
  getNodeIdsForUserInAcl,
  updateUserAclContent,
  userExistsInAclContent,
} from './aclManager.js';

const NODE_ID = 'A1'.repeat(32);

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

test('removes an existing user block when verified ownership is empty', () => {
  const content = [
    'user owner-a',
    `topic write meshcore/+/${'A'.repeat(64)}/packets`,
    `topic write meshcore/+/${'A'.repeat(64)}/status`,
    '',
    'user owner-b',
    `topic write meshcore/+/${'B'.repeat(64)}/packets`,
    '',
  ].join('\n');

  const updated = updateUserAclContent(content, 'owner-a', []);
  assert.equal(getNodeIdsForUserInAcl(updated, 'owner-a').length, 0);
  assert.deepEqual(getNodeIdsForUserInAcl(updated, 'owner-b'), ['B'.repeat(64)]);
  assert.doesNotMatch(updated, /^user owner-a$/m);
});

test('treats case-distinct MQTT usernames as separate ACL principals', () => {
  const upperNode = 'A'.repeat(64);
  const lowerNode = 'B'.repeat(64);
  const content = [
    'user Alice',
    `topic write meshcore/+/${upperNode}/packets`,
    '',
    'user alice',
    `topic write meshcore/+/${lowerNode}/packets`,
    '',
  ].join('\n');

  assert.deepEqual(getNodeIdsForUserInAcl(content, 'Alice'), [upperNode]);
  assert.deepEqual(getNodeIdsForUserInAcl(content, 'alice'), [lowerNode]);
  const updated = updateUserAclContent(content, 'alice', []);
  assert.match(updated, /user Alice/);
  assert.doesNotMatch(updated, /user alice/);
});
