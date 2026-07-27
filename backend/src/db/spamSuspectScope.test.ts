import assert from 'node:assert/strict';
import test from 'node:test';
import { assertSpamSuspectScope } from './index.js';

test('spam suspect replacement rejects rows outside its deletion scope', () => {
  assert.doesNotThrow(() => assertSpamSuspectScope(
    ['ukmesh', 'northeast', 'teesside'],
    [{ network: 'ukmesh' }, { network: 'teesside' }],
  ));
  assert.throws(
    () => assertSpamSuspectScope(['ukmesh'], [{ network: 'test' }]),
    /SPAM_SUSPECT_OUT_OF_SCOPE/,
  );
});
