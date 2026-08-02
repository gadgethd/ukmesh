import assert from 'node:assert/strict';
import test from 'node:test';
import { parseRadioBotUrl } from './radio.js';

test('radio bot integration is optional and accepts only HTTP upstreams', () => {
  assert.equal(parseRadioBotUrl(undefined), null);
  assert.equal(parseRadioBotUrl('  '), null);
  assert.equal(parseRadioBotUrl('https://radio.internal:3011').href, 'https://radio.internal:3011/');
  assert.throws(
    () => parseRadioBotUrl('file:///etc/passwd'),
    /RADIO_BOT_URL must use http or https/,
  );
});
