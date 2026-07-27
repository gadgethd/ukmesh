import assert from 'node:assert/strict';
import test from 'node:test';
import { operatorTokenIsConfigured, verifyOperatorToken } from './operatorAuth.js';

const token = '0123456789abcdef0123456789abcdef';

test('operator token requires a deployment-grade secret', () => {
  assert.equal(operatorTokenIsConfigured(undefined), false);
  assert.equal(operatorTokenIsConfigured('short'), false);
  assert.equal(operatorTokenIsConfigured(token), true);
});

test('operator token comparison rejects missing and incorrect credentials', () => {
  assert.equal(verifyOperatorToken(token, token), true);
  assert.equal(verifyOperatorToken(token, `${token}x`), false);
  assert.equal(verifyOperatorToken(token, undefined), false);
});
