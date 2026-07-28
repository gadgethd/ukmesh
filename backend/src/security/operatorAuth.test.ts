import assert from 'node:assert/strict';
import test from 'node:test';
import type { Request } from 'express';
import {
  createCsrfToken,
  operatorTokenIsConfigured,
  readCookie,
  verifyDoubleSubmitCsrf,
  verifyOperatorToken,
} from './operatorAuth.js';

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

test('double-submit CSRF requires matching cookie and header tokens', () => {
  const csrfToken = createCsrfToken();
  assert.equal(csrfToken.length >= 32, true);
  assert.equal(readCookie(`other=x; csrf=${encodeURIComponent(csrfToken)}`, 'csrf'), csrfToken);
  const request = {
    headers: {
      cookie: `csrf=${encodeURIComponent(csrfToken)}`,
      'x-csrf-token': csrfToken,
    },
  } as Request;
  assert.equal(verifyDoubleSubmitCsrf(request, 'csrf'), true);
  request.headers['x-csrf-token'] = 'incorrect';
  assert.equal(verifyDoubleSubmitCsrf(request, 'csrf'), false);
  delete request.headers['x-csrf-token'];
  assert.equal(verifyDoubleSubmitCsrf(request, 'csrf'), false);
});
