import test from 'node:test';
import assert from 'node:assert/strict';
import {
  InvalidPublicNetworkScopeError,
  PublicAllScopeForbiddenError,
  resolvePublicNetworkScope,
  validatePublicNetworkScopeForHandshake,
} from './requestScope.js';

const headers = {} as never;

test('handshake validation accepts valid, blank and missing scopes', () => {
  for (const value of ['ukmesh', 'test', 'UKMesh', '  ukmesh  ', '', undefined, null]) {
    const result = validatePublicNetworkScopeForHandshake(value, headers);
    assert.deepEqual(result, { ok: true });
  }
});

test('handshake validation rejects the all-network scope without throwing', () => {
  const result = validatePublicNetworkScopeForHandshake('all', headers);
  assert.equal(result.ok, false);
  assert.equal(result.reason, 'all_scope_forbidden');
  assert.equal(result.statusCode, 400);
});

test('handshake validation rejects invalid scopes without throwing', () => {
  for (const value of ['invalid', 'teesside-is-not-a-scope-x', 'zzz']) {
    const result = validatePublicNetworkScopeForHandshake(value, headers);
    assert.equal(result.ok, false);
    assert.equal(result.reason, 'invalid_scope');
    assert.equal(result.statusCode, 400);
  }
});

test('handshake validation treats a whitespace-padded all scope as forbidden', () => {
  const result = validatePublicNetworkScopeForHandshake(' all ', headers);
  assert.equal(result.ok, false);
  assert.equal(result.reason, 'all_scope_forbidden');
  assert.equal(result.statusCode, 400);
});

test('handshake validation never throws for unexpected resolver failures', () => {
  const hostileValue = {
    toString(): string {
      throw new Error('hostile scope coercion');
    },
  };
  let result: ReturnType<typeof validatePublicNetworkScopeForHandshake> | undefined;
  assert.doesNotThrow(() => {
    result = validatePublicNetworkScopeForHandshake(hostileValue, headers);
  });
  assert.equal(result?.ok, false);
  assert.equal(result?.reason, 'unexpected_error');
});

test('handshake validation is total across repeated hostile inputs', () => {
  for (let i = 0; i < 100; i += 1) {
    const value = i % 3 === 0 ? 'invalid' : i % 3 === 1 ? 'all' : 'ukmesh';
    assert.doesNotThrow(() => validatePublicNetworkScopeForHandshake(value, headers));
  }
});

test('resolvePublicNetworkScope still throws typed errors for callers that expect them', () => {
  assert.throws(() => resolvePublicNetworkScope('all', headers), PublicAllScopeForbiddenError);
  assert.throws(
    () => resolvePublicNetworkScope('invalid', headers),
    InvalidPublicNetworkScopeError,
  );
});
