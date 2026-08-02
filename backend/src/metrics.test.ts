import assert from 'node:assert/strict';
import test from 'node:test';
import {
  boundedAnalysisWorkloadLabel,
  boundedNetworkMetricLabel,
  metricsRegistry,
} from './metrics.js';

const ALLOWED_LABEL_NAMES = new Set([
  'cache',
  'check',
  'dataset',
  'method',
  'network',
  'operation',
  'outcome',
  'phase',
  'pool',
  'state',
  'status',
  'status_class',
  'kind',
  'major',
  'minor',
  'patch',
  'space',
  'type',
  'version',
  'worker',
  'workload',
]);

test('metrics use only reviewed low-cardinality labels and fit the scrape budget', async () => {
  for (const metric of metricsRegistry.getMetricsAsArray()) {
    const labelNames = (
      metric as unknown as { labelNames?: string[] }
    ).labelNames ?? [];
    const maxLabels = metric.name.startsWith('meshcore_process_') ? 4 : 3;
    assert.ok(labelNames.length <= maxLabels, `${metric.name} has too many labels`);
    for (const label of labelNames) {
      assert.ok(ALLOWED_LABEL_NAMES.has(label), `${metric.name} has unreviewed label ${label}`);
    }
  }
  const scrape = await metricsRegistry.metrics();
  assert.ok(Buffer.byteLength(scrape) <= 512 * 1024, 'metrics scrape exceeds 512 KiB');
  for (const forbidden of ['node_id=', 'owner=', 'webhook_url=', 'packet_hash=', 'exception=']) {
    assert.equal(scrape.includes(forbidden), false, `scrape leaked high-cardinality label ${forbidden}`);
  }
});

test('dynamic metric dimensions collapse unknown values into bounded buckets', () => {
  assert.equal(boundedNetworkMetricLabel('ukmesh'), 'ukmesh');
  assert.equal(boundedNetworkMetricLabel('customer-created-network'), 'other');
  assert.equal(boundedAnalysisWorkloadLabel('path-history'), 'path-history');
  assert.equal(boundedAnalysisWorkloadLabel('run-123456'), 'other');
});
