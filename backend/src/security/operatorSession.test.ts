import assert from 'node:assert/strict';
import test from 'node:test';
import { OperatorSessionStore } from './operatorSession.js';

test('operator sessions expire, enforce CSRF, and remain capacity bounded', () => {
  let now = 1_000;
  const store = new OperatorSessionStore(100, 2, () => now);
  const first = store.create();
  assert.equal(store.get(first.id)?.id, first.id);
  assert.equal(store.verifyCsrf(first, first.csrfToken), true);
  assert.equal(store.verifyCsrf(first, 'wrong'), false);
  store.create();
  store.create();
  assert.equal(store.get(first.id), null);
  assert.equal(store.size(), 2);
  now += 101;
  assert.equal(store.size(), 0);
});
