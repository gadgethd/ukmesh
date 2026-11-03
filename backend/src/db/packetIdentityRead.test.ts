import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import { getRecentMessages, getRecentPackets } from './index.js';
import { packetIdentityKeySql, packetIdentityReadAuthority } from './packetIdentityRead.js';

test('packet identity read helper validates authority and aliases', () => {
  assert.equal(packetIdentityReadAuthority({}), 'uid');
  assert.equal(packetIdentityReadAuthority({ PACKET_IDENTITY_READ_AUTHORITY: 'hash' }), 'hash');
  assert.throws(
    () => packetIdentityReadAuthority({ PACKET_IDENTITY_READ_AUTHORITY: 'legacy' }),
    /must be "uid" or "hash"/,
  );
  assert.equal(packetIdentityKeySql('p', 'hash'), 'p.packet_hash');
  assert.equal(
    packetIdentityKeySql('p', 'uid'),
    "COALESCE(encode(p.packet_uid, 'hex'), p.packet_hash)",
  );
  assert.throws(() => packetIdentityKeySql('p; DROP TABLE mesh.packets', 'uid'), /unsafe SQL alias/);
});

test('recent readers switch identity authority without changing the public hash field', async (t) => {
  const calls: string[] = [];
  t.mock.method(pg.Pool.prototype, 'query', async (sql: string) => {
    calls.push(String(sql));
    return { rows: [] };
  });
  const previous = process.env['PACKET_IDENTITY_READ_AUTHORITY'];
  try {
    process.env['PACKET_IDENTITY_READ_AUTHORITY'] = 'uid';
    await getRecentPackets(3, 'ukmesh');
    assert.match(
      calls.at(-1)!,
      /DISTINCT ON \(COALESCE\(encode\(p\.packet_uid, 'hex'\), p\.packet_hash\)\)/,
    );
    assert.match(
      calls.at(-1)!,
      /ORDER BY COALESCE\(encode\(p\.packet_uid, 'hex'\), p\.packet_hash\),/,
    );
    assert.match(calls.at(-1)!, /p\.packet_hash/);

    await getRecentMessages(3, 'ukmesh');
    assert.match(
      calls.at(-1)!,
      /DISTINCT ON \(COALESCE\(encode\(p\.packet_uid, 'hex'\), p\.packet_hash\)\)/,
    );

    process.env['PACKET_IDENTITY_READ_AUTHORITY'] = 'hash';
    await getRecentPackets(3, 'ukmesh');
    assert.match(calls.at(-1)!, /DISTINCT ON \(p\.packet_hash\)/);
    assert.doesNotMatch(calls.at(-1)!, /encode\(p\.packet_uid/);
  } finally {
    if (previous === undefined) delete process.env['PACKET_IDENTITY_READ_AUTHORITY'];
    else process.env['PACKET_IDENTITY_READ_AUTHORITY'] = previous;
  }
});
