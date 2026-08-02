import assert from 'node:assert/strict';
import test from 'node:test';
import {
  LifecycleCoordinator,
  LifecycleDeadlineError,
} from './coordinator.js';

test('lifecycle drain is idempotent and closes stages in order', async () => {
  const lifecycle = new LifecycleCoordinator(1_000);
  const events: string[] = [];
  lifecycle.register({
    name: 'network',
    stage: 20,
    close: () => { events.push('network'); },
  });
  lifecycle.register({
    name: 'admission',
    stage: 10,
    close: async () => {
      await new Promise((resolve) => setImmediate(resolve));
      events.push('admission');
    },
  });
  const first = lifecycle.drain('SIGTERM');
  const second = lifecycle.drain('SIGINT');
  assert.equal(first, second);
  assert.equal(lifecycle.isDraining, true);
  await first;
  assert.deepEqual(events, ['admission', 'network']);
  assert.deepEqual(lifecycle.snapshot(), {
    draining: true,
    reason: 'SIGTERM',
    outstanding: [],
  });
});

test('lifecycle deadline reports the outstanding resource', async () => {
  const lifecycle = new LifecycleCoordinator(20);
  lifecycle.register({
    name: 'stuck-resource',
    stage: 10,
    close: () => new Promise(() => {}),
  });
  await assert.rejects(
    lifecycle.drain('test'),
    (error: unknown) => (
      error instanceof LifecycleDeadlineError
      && error.outstanding.includes('stuck-resource')
    ),
  );
});
