import assert from 'node:assert/strict';
import test from 'node:test';
import { isOwnerPacketSharingResponse, isOwnerPublicFeedResponse, isPacketSharingOwner } from './ownerPortalModel.js';

const valid = {
  nodeId: 'A'.repeat(64),
  featureEnabled: true,
  enabled: false,
  destinations: [{
    id: 'remote-site',
    name: 'Remote site',
    websiteUrl: 'https://remote.example.test',
    description: 'A remote packet site',
    configured: true,
    selected: false,
    lastForwardedAt: null,
  }],
};

test('owner packet-sharing response guard accepts the bounded settings DTO', () => {
  assert.equal(isOwnerPacketSharingResponse(valid), true);
  assert.equal(isOwnerPacketSharingResponse({ ...valid, nodeId: 'short' }), false);
  assert.equal(isOwnerPacketSharingResponse({ ...valid, destinations: [{ ...valid.destinations[0], id: 'Remote Site' }] }), false);
  assert.equal(isOwnerPacketSharingResponse({ ...valid, destinations: [{ ...valid.destinations[0], selected: 'yes' }] }), false);
  assert.equal(isOwnerPacketSharingResponse({ ...valid, destinations: [{ ...valid.destinations[0], websiteUrl: 'javascript:alert(1)' }] }), false);
  assert.equal(isOwnerPacketSharingResponse({ ...valid, destinations: [{ ...valid.destinations[0], lastForwardedAt: 'not-a-date' }] }), false);
  assert.equal(isOwnerPacketSharingResponse({ ...valid, featureEnabled: 'true' }), false);
});

test('shows packet sharing for every authenticated owner account', () => {
  assert.equal(isPacketSharingOwner('node1'), true);
  assert.equal(isPacketSharingOwner('gnomemoe'), true);
  assert.equal(isPacketSharingOwner('node2'), true);
  assert.equal(isPacketSharingOwner('new-owner'), true);
  assert.equal(isPacketSharingOwner(null), false);
});

test('owner public-feed response guard accepts the account-level settings DTO', () => {
  const valid = {
    featureEnabled: true,
    configured: true,
    brokerUrl: 'wss://mqtt.example.test:443',
    enabled: false,
    nodeCount: 2,
    eligible: true,
    recentPacketCount: 1,
    topicFilters: ['public/+/+/packets'],
    lastForwardedAt: null,
  };
  assert.equal(isOwnerPublicFeedResponse(valid), true);
  assert.equal(isOwnerPublicFeedResponse({ ...valid, nodeCount: -1 }), false);
  assert.equal(isOwnerPublicFeedResponse({ ...valid, topicFilters: ['meshcore/#'] }), false);
  assert.equal(isOwnerPublicFeedResponse({ ...valid, lastForwardedAt: 'invalid' }), false);
});
