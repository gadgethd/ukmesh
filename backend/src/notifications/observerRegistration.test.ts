import assert from 'node:assert/strict';
import test from 'node:test';
import { observerRegistrationWebhookDeliveriesTotal } from '../metrics.js';
import {
  notifyObserverRegistration,
  observerRegistrationPayload,
} from './observerRegistration.js';

const sample = {
  requestId: '4242',
  publicKey: 'AB'.repeat(32),
  iata: 'NCL',
  name: 'Test Observer',
  contact: 'test@example.invalid',
};

async function failureCount(): Promise<number> {
  const metric = await observerRegistrationWebhookDeliveriesTotal.get();
  return metric.values
    .filter((value) => value.labels['outcome'] === 'failure')
    .reduce((total, value) => total + value.value, 0);
}

test('registration payload never allows mentions', () => {
  const payload = observerRegistrationPayload(sample, new Date('2026-11-15T00:00:00Z'));
  assert.deepEqual(payload['allowed_mentions'], { parse: [] });
  const embeds = payload['embeds'] as Array<{
    title: string;
    fields: Array<{ name: string; value: string }>;
  }>;
  assert.equal(embeds[0].title, 'New observer registration #4242');
  assert.ok(
    embeds[0].fields.some(
      (field) => field.name === 'Contact' && field.value === sample.contact,
    ),
  );
});

test('failed deliveries are counted and never throw', async () => {
  const before = await failureCount();
  const previous = process.env['OBSERVER_REGISTRATION_WEBHOOK_URL'];
  // Loopback is rejected by the outbound webhook address policy, so both
  // attempts fail without any network egress.
  process.env['OBSERVER_REGISTRATION_WEBHOOK_URL'] = 'https://127.0.0.1/observer-registration-test';
  try {
    notifyObserverRegistration(sample);
    const deadline = Date.now() + 5_000;
    let after = await failureCount();
    while (after < before + 2 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      after = await failureCount();
    }
    assert.ok(after >= before + 2, `expected two failed attempts, saw ${after - before}`);
  } finally {
    if (previous === undefined) delete process.env['OBSERVER_REGISTRATION_WEBHOOK_URL'];
    else process.env['OBSERVER_REGISTRATION_WEBHOOK_URL'] = previous;
  }
});

test('an unset webhook URL is a no-op', async () => {
  const before = await failureCount();
  const previous = process.env['OBSERVER_REGISTRATION_WEBHOOK_URL'];
  delete process.env['OBSERVER_REGISTRATION_WEBHOOK_URL'];
  try {
    notifyObserverRegistration(sample);
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(await failureCount(), before);
  } finally {
    if (previous !== undefined) process.env['OBSERVER_REGISTRATION_WEBHOOK_URL'] = previous;
  }
});
