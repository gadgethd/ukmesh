import assert from 'node:assert/strict';
import test from 'node:test';
import type { IncomingMessage } from 'node:http';
import { trustedClientIp } from './trustedProxy.js';

function request(peer: string, forwarded?: string): IncomingMessage {
  return {
    socket: { remoteAddress: peer },
    headers: forwarded ? { 'x-forwarded-for': forwarded } : {},
  } as unknown as IncomingMessage;
}

test('untrusted socket peers cannot spoof a forwarded client address', () => {
  assert.equal(trustedClientIp(request('203.0.113.8', '198.51.100.10')), '203.0.113.8');
});

test('an exact trusted peer may supply one validated client address', () => {
  assert.equal(trustedClientIp(request('127.0.0.1', '198.51.100.10')), '198.51.100.10');
  assert.equal(trustedClientIp(request('127.0.0.1', 'not-an-ip')), '127.0.0.1');
});
