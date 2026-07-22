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
