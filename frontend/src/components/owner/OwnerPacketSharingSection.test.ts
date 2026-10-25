import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { OwnerPacketSharingSection } from './OwnerPortalSections.js';

const settings = {
  nodeId: 'A'.repeat(64),
  featureEnabled: true,
  enabled: false,
  destinations: [{
    id: 'relaylens',
    name: 'RelayLens',
    websiteUrl: 'https://destination.example.test',
    description: 'Explore how MeshCore messages travel.',
    configured: true,
    selected: false,
    lastForwardedAt: null,
  }],
};

function section(featureEnabled = true) {
  return renderToStaticMarkup(React.createElement(OwnerPacketSharingSection, {
    settings: { ...settings, featureEnabled },
    loading: false,
    saving: false,
    error: null,
    selectedDestinationIds: [],
    onDestination: () => {},
  }));
}

test('packet sharing renders one independent iOS-style switch per destination', () => {
  const html = section();
  assert.match(html, /Destination controls/);
  assert.match(html, /class="owner-sharing-switch"/);
  assert.match(html, /role="switch"/);
  assert.equal((html.match(/type="checkbox"/g) ?? []).length, 1);
  assert.match(html, /href="https:\/\/destination\.example\.test"/);
  assert.match(html, /title="Explore how MeshCore messages travel\."/);
  assert.match(html, />RelayLens<\/a>/);
  assert.match(html, /Ready to enable/);
});

test('packet sharing reports a successful packet delivery for the destination', () => {
  const html = renderToStaticMarkup(React.createElement(OwnerPacketSharingSection, {
    settings: {
      ...settings,
      enabled: true,
      destinations: [{ ...settings.destinations[0], selected: true, lastForwardedAt: '2026-09-16T22:45:00.000Z' }],
    },
    loading: false,
    saving: false,
    error: null,
    selectedDestinationIds: ['relaylens'],
    onDestination: () => {},
  }));
  assert.match(html, /Last packet forwarded successfully/);
  assert.doesNotMatch(html, /Waiting for first packet/);
});

test('packet sharing keeps each destination switch independent', () => {
  const html = renderToStaticMarkup(React.createElement(OwnerPacketSharingSection, {
    settings: {
      ...settings,
      destinations: [
        ...settings.destinations,
        {
          id: 'another-site',
          name: 'Another site',
          websiteUrl: null,
          description: null,
          configured: true,
          selected: true,
          lastForwardedAt: null,
        },
      ],
    },
    loading: false,
    saving: false,
    error: null,
    selectedDestinationIds: ['another-site'],
    onDestination: () => {},
  }));
  assert.equal((html.match(/role="switch"/g) ?? []).length, 2);
  assert.match(html, /checked=""/);
});

test('packet sharing stays hidden when the feature is disabled', () => {
  assert.doesNotMatch(section(false), /Share packets/);
});
