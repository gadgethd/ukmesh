import assert from 'node:assert/strict';
import test from 'node:test';
import {
  networksSharePrivacyScope,
  privateNodePacketNetworkMatchSql,
} from './networkScope.js';

test('privacy scope preserves production aliases while isolating test', () => {
  assert.equal(networksSharePrivacyScope('ukmesh', 'teesside'), true);
  assert.equal(networksSharePrivacyScope('northeast', 'ukmesh'), true);
  assert.equal(networksSharePrivacyScope('test', 'test'), true);
  assert.equal(networksSharePrivacyScope('test', 'ukmesh'), false);
  assert.equal(networksSharePrivacyScope('ukmesh', 'test'), false);
});

test('privacy SQL aliases are internal identifiers only', () => {
  assert.match(
    privateNodePacketNetworkMatchSql('private_node', 'p'),
    /p\.is_private IS TRUE/,
  );
  assert.throws(
    () => privateNodePacketNetworkMatchSql('private_node', 'p; DROP TABLE packets'),
    /INVALID_SQL_ALIAS/,
  );
});
