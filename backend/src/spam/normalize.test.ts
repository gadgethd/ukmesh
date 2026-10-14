import { test } from 'node:test';
import assert from 'node:assert/strict';
import { normalizeMessage, normalizeUsername, canonicalizeUrl, collapseRepeats, looksMachineGeneratedName, SPAM_MARKER } from './normalize.js';

test('lowercases, trims and collapses whitespace', () => {
  const n = normalizeMessage('  Hello   WORLD  ');
  assert.equal(n.normalized, 'hello world');
  assert.deepEqual(n.tokens, ['hello', 'world']);
});

test('collapses stretched repeated characters', () => {
  assert.equal(collapseRepeats('heyyyyy'), 'heyy');
  assert.equal(normalizeMessage('soooo coooool!!!').normalized, 'soo cool');
});

test('normalizes fancy punctuation and strips symbols', () => {
  const n = normalizeMessage('“Buy — now”, it’s great!!!');
  assert.equal(n.normalized, 'buy now it s great');
});

test('canonicalizeUrl drops scheme, www, query and trailing punctuation', () => {
  assert.equal(canonicalizeUrl('https://www.Example.com/Path/?utm=1#frag'), 'example.com/path');
  assert.equal(canonicalizeUrl('HTTP://foo.org/'), 'foo.org');
  assert.equal(canonicalizeUrl('bar.net/x).'), 'bar.net/x');
});

test('extracts and canonicalizes URLs so tracking variants collapse', () => {
  const a = normalizeMessage('check https://www.shop.com/deal?ref=aaa now');
  const b = normalizeMessage('check shop.com/deal?ref=zzz now');
  assert.ok(a.urls.includes('shop.com/deal'));
  assert.ok(b.urls.includes('shop.com/deal'));
  assert.equal(a.normalized, b.normalized);
});

test('detects the ukmesh.com/spam marker in several forms', () => {
  assert.equal(normalizeMessage(`see ${SPAM_MARKER} lol`).hasSpamMarker, true);
  assert.equal(normalizeMessage('go to HTTPS://ukmesh.com/spam').hasSpamMarker, true);
  assert.equal(normalizeMessage('visit www.ukmesh.com/spam/abuse').hasSpamMarker, true);
  assert.equal(normalizeMessage('nothing suspicious here').hasSpamMarker, false);
});

test('marker is not the only signal — normal text still normalizes', () => {
  const n = normalizeMessage('totally normal message');
  assert.equal(n.hasSpamMarker, false);
  assert.equal(n.normalized, 'totally normal message');
});

test('normalizeUsername strips emoji, case and separators', () => {
  assert.equal(normalizeUsername('John_UK 📻'), 'john uk');
  assert.equal(normalizeUsername('  Bob!!  '), 'bob');
  assert.equal(normalizeUsername('G3KFB Nick 🐵'), 'g3kfb nick');
});

test('handles null / empty input safely', () => {
  const n = normalizeMessage(null);
  assert.equal(n.normalized, '');
  assert.deepEqual(n.tokens, []);
  assert.equal(normalizeUsername(undefined), '');
});

test('machine-generated names are detected (keygen-style botnet ids)', () => {
  // Observed rotating-ID spammer style: separator-free base32-ish tokens.
  for (const id of ['0CYT64KV', '1KG3RWU7', '43DBCEEF5C', '5ZA1J31M', '16V0OP2W', 'H5M6N2P8', 'BKGL7DMF', '8SPLFZAP']) {
    assert.equal(looksMachineGeneratedName(id), true, `${id} should look machine-generated`);
  }
});

test('human and amateur-radio handles are not machine-generated', () => {
  for (const name of [
    'M7GQW', 'MU6GBG', 'G0SYR', '2E0ABC', 'GW4ABC', 'GB3WF',
    'Rob M7JCN V3', 'Dave-S75', 'Grog 🚲', 'Simeon', 'Timberland',
    'Delta4zer0', 'eyrie', 'Alice', '12345678', 'test',
  ]) {
    assert.equal(looksMachineGeneratedName(name), false, `${name} should not look machine-generated`);
  }
});
