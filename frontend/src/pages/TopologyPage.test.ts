import assert from 'node:assert/strict';
import { registerHooks } from 'node:module';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';

// Render the real RF panel without loading the browser's map or Vite env.
// Request, retry and cleanup behavior is exercised in the browser test.
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier.endsWith('.css')) return { url: 'data:text/javascript,export default {}', shortCircuit: true };
    if (specifier === '../components/Map/TopologyMap.js') {
      return { url: 'data:text/javascript,export const TopologyMap=()=>null', shortCircuit: true };
    }
    if (specifier === '../config/site.js') {
      return { url: 'data:text/javascript,export const getCurrentSite=()=>({network:"ukmesh"})', shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});
const { RfValidationPanel } = await import('./TopologyPage.js');
hooks.deregister();
const render = (data: Parameters<typeof RfValidationPanel>[0]['data'], unavailable = false) => (
  renderToStaticMarkup(React.createElement(RfValidationPanel, { data, unavailable, onRetry: () => {} }))
);

test('RF panel remains visible while loading and on failure with an accessible retry action', () => {
  const loading = render(null);
  assert.match(loading, /Predicted versus observed RF/);
  assert.match(loading, /Loading RF validation/);
  const failed = render(null, true);
  assert.match(failed, /Predicted versus observed RF/);
  assert.match(failed, /role="alert"/);
  assert.match(failed, /RF validation is temporarily unavailable/);
  assert.match(failed, /<button type="button">Retry RF validation<\/button>/);
  assert.doesNotMatch(failed, /links evaluated/);
});

test('RF success retains real counts, mismatch details and a distinct empty-data state', () => {
  const data = { methodology: 'Fixture methodology',
    summary: { evaluated: 25, matches: 24, mismatches: 1, observedUnexpected: 1, operatorOverrides: 0, weakModelEvidence: 0 },
    mismatches: [{ source: 'A', target: 'B', sourceName: '<private text>', targetName: null,
      observations: 12, strongObservations: 3, pathLossDb: 130,
      classification: 'observed_unexpected' as const }] };
  const html = render(data);
  assert.match(html, /25<\/strong><span>links evaluated/);
  assert.match(html, /RF model mismatches/);
  assert.match(html, /130.0 dB/);
  assert.match(html, /&lt;private text&gt;/);
  assert.doesNotMatch(html, /temporarily unavailable/);
  assert.match(render({ ...data, summary: { ...data.summary, evaluated: 0 }, mismatches: [] }), /No recent public repeater links/);
});
