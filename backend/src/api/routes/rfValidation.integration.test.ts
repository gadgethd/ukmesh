import assert from 'node:assert/strict';
import test from 'node:test';

// Opt in only after the authorized backend deployment. This is a GET against
// the actual route and its bounded read-only pool, without any DB writes.
test('live UKMesh RF validation returns populated results within five seconds at every supported size', {
  skip: process.env['AUDITWAVE_LIVE_TESTS'] !== '1',
  timeout: 20_000,
}, async () => {
  for (const limit of [25, 100, 250]) {
    const startedAt = performance.now();
    const response = await fetch(`http://127.0.0.1:3000/api/rf-validation?network=ukmesh&limit=${limit}`, {
      signal: AbortSignal.timeout(5_000),
    });
    const body = await response.json() as { summary: { evaluated: number } };
    const elapsedMs = performance.now() - startedAt;
    assert.equal(response.status, 200);
    assert.ok(elapsedMs < 5_000, `RF validation took ${elapsedMs.toFixed(0)}ms`);
    assert.ok(body.summary.evaluated > 0 && body.summary.evaluated <= limit);
    console.log(JSON.stringify({ limit, elapsedMs, evaluated: body.summary.evaluated }));
  }
});
