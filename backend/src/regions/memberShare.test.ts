import assert from 'node:assert/strict';
import test from 'node:test';
import { MEMBER_MIN_SHARE_PCT, meetsMemberShare } from './memberShare.js';

test('MEMBER_MIN_SHARE_PCT is the calibrated 10% floor', () => {
  assert.equal(MEMBER_MIN_SHARE_PCT, 10);
});

test('meetsMemberShare accepts the floor exactly and anything above it', () => {
  assert.equal(meetsMemberShare(10, 100), true);
  assert.equal(meetsMemberShare(11, 100), true);
  assert.equal(meetsMemberShare(1, 10), true);
  // yorkshire-scale: 9599 packets, floor lands at 960
  assert.equal(meetsMemberShare(960, 9599), true);
  assert.equal(meetsMemberShare(7065, 9599), true);
});

test('meetsMemberShare rejects token relays below the floor', () => {
  assert.equal(meetsMemberShare(9, 100), false);
  assert.equal(meetsMemberShare(959, 9599), false);
  assert.equal(meetsMemberShare(1, 1000), false);
  assert.equal(meetsMemberShare(0, 100), false);
});

test('meetsMemberShare is defensive about degenerate inputs', () => {
  assert.equal(meetsMemberShare(5, 0), false);
  assert.equal(meetsMemberShare(0, 0), false);
  assert.equal(meetsMemberShare(Number.NaN, 100), false);
  assert.equal(meetsMemberShare(5, Number.NaN), false);
  assert.equal(meetsMemberShare(5, Number.POSITIVE_INFINITY), false);
});
