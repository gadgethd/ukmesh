import assert from 'node:assert/strict';
import test from 'node:test';
import { isIncidentDetail, isIncidentsResp, isStatusResp } from './payload.js';

const incident = {
  id: 'synthetic-incident', network: 'ukmesh', status: 'active',
  firstSeen: '2026-01-01T00:00:00Z', lastSeen: '2026-01-01T01:00:00Z',
  messageCount: 10, observerCount: 3, usernameVariants: 1, channels: ['Public'],
  similarUsernames: ['Invented relay'], sampleMessage: 'Invented fixture content',
  spamMarker: false, confidence: 0.9, reasons: ['Repeated invented content'],
  origin: { region: 'Synthetic region', confidence: 0.7, level: 'medium', observerCount: 3,
    reasons: [], zone: { lat: 54, lon: -1, radiusKm: 10 } },
};

test('spam responses reject malformed geometry, nested arrays and invalid timeline values', () => {
  const list = { filters: { status: 'all', minConfidence: 0.5, limit: 100, offset: 0 },
    returned: 1, incidents: [incident] };
  assert.ok(isIncidentsResp(list));
  assert.equal(isIncidentsResp({ ...list, incidents: [{ ...incident, origin: null }] }), false);
  assert.equal(isIncidentsResp({ ...list, incidents: [{ ...incident, channels: [3] }] }), false);
  assert.equal(isIncidentsResp({ ...list, incidents: [{ ...incident,
    origin: { ...incident.origin, zone: { lat: 100, lon: -1, radiusKm: 10 } } }] }), false);
  const timeline = [{ observedAt: '2026-01-01T00:00:00Z', channel: 'Public', observerCount: 3,
    minHopCount: null, bestRssi: -80, bestSnr: 4 }];
  assert.ok(isIncidentDetail({ ...incident, timeline }));
  assert.equal(isIncidentDetail({ ...incident, timeline: [{ ...timeline[0], bestSnr: '4' }] }), false);
  assert.equal(isIncidentDetail({ ...incident, timeline: [{ ...timeline[0], observedAt: 'invalid' }] }), false);
});

test('spam status accepts empty counts and rejects negative counts and invalid dates', () => {
  const status = { ongoing: false, activeIncidents: 0, totalIncidents: 0, messagesLast24h: 0,
    observersInvolved: 0, lastIncidentAt: null, updatedAt: '2026-01-01T00:00:00Z' };
  assert.ok(isStatusResp(status));
  assert.equal(isStatusResp({ ...status, activeIncidents: -1 }), false);
  assert.equal(isStatusResp({ ...status, lastIncidentAt: 'invalid' }), false);
});
