import assert from 'node:assert/strict';
import test from 'node:test';
import type { IncomingMessage } from 'node:http';
import {
  isTrustedProxyPeer,
  reloadTrustedProxyPeersForTest,
  trustedClientIp,
} from './trustedProxy.js';

function request(peer: string, forwarded?: string): IncomingMessage {
  return {
    socket: { remoteAddress: peer },
    headers: forwarded ? { 'x-forwarded-for': forwarded } : {},
  } as IncomingMessage;
}

test('forwarded identity is accepted only from an exact configured peer', () => {
  reloadTrustedProxyPeersForTest(['172.30.0.10']);

  assert.equal(isTrustedProxyPeer('::ffff:172.30.0.10'), true);
  assert.equal(trustedClientIp(request('172.30.0.10', '203.0.113.10')), '203.0.113.10');
  assert.equal(trustedClientIp(request('172.30.0.99', '203.0.113.10')), '172.30.0.99');
});

test('trusted proxy consumes one overwritten value, not an appended chain', () => {
  reloadTrustedProxyPeersForTest(['172.30.0.10']);
  assert.equal(
    trustedClientIp(request('172.30.0.10', '198.51.100.7, 10.0.0.2')),
    '198.51.100.7',
  );
  assert.equal(trustedClientIp(request('172.30.0.10', 'not-an-ip')), '172.30.0.10');
});
