import assert from 'node:assert/strict';
import test from 'node:test';
import { parseMqttTopic } from './topic.js';

const observer = 'a'.repeat(64);
const prefixes = new Set(['meshcore', 'ukmesh', 'meshcore-test', 'lab']);
const blocked = new Set(['TST']);

test('classifies public and configured test MQTT topic prefixes without mixing scopes', () => {
  assert.deepEqual(
    parseMqttTopic(`meshcore/lhr/${observer}/packets`, prefixes, blocked),
    { iata: 'LHR', observerKey: observer.toUpperCase(), suffix: 'packets', network: 'ukmesh' },
  );
  assert.deepEqual(
    parseMqttTopic(`meshcore-test/test/${observer.toUpperCase()}/status`, prefixes, blocked),
    { iata: 'TEST', observerKey: observer.toUpperCase(), suffix: 'status', network: 'test' },
  );
  assert.equal(
    parseMqttTopic(`lab/abc/${observer}/packets`, prefixes, blocked)?.network,
    'test',
  );
});

test('rejects malformed, blocked, and unsupported MQTT topics before persistence', () => {
  assert.equal(parseMqttTopic(`meshcore/TST/${observer}/packets`, prefixes, blocked), null);
  assert.equal(parseMqttTopic('meshcore/lhr/short/packets', prefixes, blocked), null);
  assert.equal(parseMqttTopic(`meshcore/lhr/${observer}/other`, prefixes, blocked), null);
  assert.equal(parseMqttTopic(`unknown/lhr/${observer}/packets`, prefixes, blocked), null);
  assert.equal(parseMqttTopic(`meshcore/lhr/${observer}/packets/extra`, prefixes, blocked), null);
});
