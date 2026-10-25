import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { OwnerPublicFeedSection } from './OwnerPortalSections.js';

const settings = {
  featureEnabled: true,
  configured: true,
  brokerUrl: 'wss://mqtt.example.test:443',
  enabled: false,
  nodeCount: 2,
  eligible: true,
  recentPacketCount: 3,
  topicFilters: ['public/+/+/packets'],
  lastForwardedAt: null,
};

function section(overrides: Partial<Omit<typeof settings, 'lastForwardedAt'>> & { lastForwardedAt?: string | null } = {}) {
  return renderToStaticMarkup(React.createElement(OwnerPublicFeedSection, {
    settings: { ...settings, ...overrides },
    loading: false,
    saving: false,
    error: null,
    onEnabled: () => undefined,
  }));
}

test('public feed renders one account-level iOS-style switch with the internet warning', () => {
  const html = section();
  assert.match(html, />Public feed</);
  assert.equal((html.match(/role="switch"/g) ?? []).length, 1);
  assert.match(html, /Packets will be observable over the internet/);
  assert.match(html, /site operator controls MQTT/);
  assert.match(html, /same MQTT username and password/);
  assert.match(html, /read-only/);
  assert.match(html, /public\/\+\/\+\/packets/);
  assert.match(html, /Public feed is off/);
});

test('public feed reports successful forwarding after the first delivery', () => {
  const html = section({ enabled: true, lastForwardedAt: '2026-09-20T12:00:00.000Z' });
  assert.match(html, /Public feed is on/);
  assert.match(html, /read-only access to the curated opted-in packet feed/);
  assert.match(html, /wss:\/\/mqtt\.example\.test:443/);
  assert.match(html, /checked=""/);
});

test('public feed switch stays off and disabled until operator broker configuration exists', () => {
  const html = section({ configured: false, enabled: false });
  assert.match(html, /The operator has not configured the public broker yet/);
  assert.match(html, /disabled=""/);
});

test('public feed switch stays locked until recent eligible activity exists', () => {
  const html = section({ eligible: false, recentPacketCount: 0 });
  assert.match(html, /Public feed is locked until you are active/);
  assert.match(html, /Publish at least one eligible packet in the last 24 hours/);
  assert.match(html, /disabled=""/);
});

test('an enabled account can turn the feed off after activity expires', () => {
  const html = section({ enabled: true, eligible: false, recentPacketCount: 0 });
  assert.match(html, /Public feed access is paused/);
  assert.doesNotMatch(html, /Public feed is locked until you are active/);
  assert.doesNotMatch(html, /disabled=""/);
});
