import assert from 'node:assert/strict';
import test from 'node:test';
import { createMqttCredentialVerifier } from './mqttCredentialVerifier.js';

test('rechecks unchanged MQTT credentials against the broker on every login', async () => {
  let checks = 0;
  const verify = createMqttCredentialVerifier(async (username, password) => {
    checks++;
    assert.equal(username, 'owner');
    assert.equal(password, 'unchanged-password');
    return checks === 1;
  });

  assert.equal(await verify('owner', 'unchanged-password'), true);
  assert.equal(await verify('owner', 'unchanged-password'), false);
  assert.equal(checks, 2);
});
