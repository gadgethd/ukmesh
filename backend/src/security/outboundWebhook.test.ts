import assert from 'node:assert/strict';
import test from 'node:test';
import {
  isPublicWebhookAddress,
  resolveWebhookTarget,
  type WebhookDnsResolver,
} from './outboundWebhook.js';

const resolver = (v4: string[] = [], v6: string[] = []): WebhookDnsResolver => ({
  async resolve4() { return v4; },
  async resolve6() { return v6; },
});

test('webhook address policy rejects private, reserved, mapped and multicast ranges', () => {
  for (const address of [
    '0.0.0.0', '10.1.2.3', '100.64.0.1', '127.0.0.1', '169.254.169.254',
    '172.16.0.1', '192.168.1.1', '192.0.2.1', '198.51.100.1',
    '203.0.113.1', '224.0.0.1', '255.255.255.255', '::', '::1',
    '::ffff:127.0.0.1', 'fc00::1', 'fe80::1', 'ff00::1', '2001:db8::1',
  ]) {
    assert.equal(isPublicWebhookAddress(address), false, address);
  }
  assert.equal(isPublicWebhookAddress('8.8.8.8'), true);
  assert.equal(isPublicWebhookAddress('2606:4700:4700::1111'), true);
});

test('webhook URL policy rejects parser ambiguity and unsafe URL features', async () => {
  for (const url of [
    'http://example.com/hook',
    'https://user:pass@example.com/hook',
    'https://example.com:8443/hook',
    'https://example.com/hook#fragment',
    'https://127.0.0.1/hook',
    'https://2130706433/hook',
    'https://[::ffff:127.0.0.1]/hook',
  ]) {
    await assert.rejects(resolveWebhookTarget(url, resolver(['8.8.8.8'])), /WEBHOOK_/);
  }
});

test('webhook DNS policy fails closed if any answer is non-public', async () => {
  await assert.rejects(
    resolveWebhookTarget('https://hooks.example/hook', resolver(['8.8.8.8', '10.0.0.1'])),
    /WEBHOOK_ADDRESS_FORBIDDEN/,
  );
  const target = await resolveWebhookTarget(
    'https://hooks.example/hook',
    resolver(['8.8.8.8'], ['2606:4700:4700::1111']),
  );
  assert.deepEqual(target.addresses, [
    { address: '8.8.8.8', family: 4 },
    { address: '2606:4700:4700::1111', family: 6 },
  ]);
});
