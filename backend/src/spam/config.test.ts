import assert from 'node:assert/strict';
import test from 'node:test';
import { loadSpamMessageConfig } from './config.js';

test('production defaults add rotating-ID headroom while preserving the ordinary floor and hard cap', (t) => {
  const keys = [
    'SPAM_MESSAGE_MAX_MESSAGES', 'SPAM_MESSAGE_MIN_TRANSMISSIONS',
    'SPAM_MESSAGE_ROTATING_MIN_TRANSMISSIONS', 'SPAM_MESSAGE_ROTATING_MACHINE_NAME_SHARE',
  ];
  for (const key of keys) {
    const before = process.env[key];
    delete process.env[key];
    t.after(() => { if (before === undefined) delete process.env[key]; else process.env[key] = before; });
  }
  const cfg = loadSpamMessageConfig();
  assert.equal(cfg.maxMessagesPerRun, 5_000);
  assert.equal(cfg.minTransmissions, 8);
  assert.equal(cfg.rotatingMinTransmissions, 6);
  assert.equal(cfg.rotatingMachineNameShare, 0.3);
  process.env['SPAM_MESSAGE_MAX_MESSAGES'] = '50000';
  assert.equal(loadSpamMessageConfig().maxMessagesPerRun, 10_000);
  process.env['SPAM_MESSAGE_MAX_MESSAGES'] = '2000';
  process.env['SPAM_MESSAGE_ROTATING_MIN_TRANSMISSIONS'] = '7';
  process.env['SPAM_MESSAGE_ROTATING_MACHINE_NAME_SHARE'] = '0.5';
  const overridden = loadSpamMessageConfig();
  assert.equal(overridden.maxMessagesPerRun, 2_000);
  assert.equal(overridden.rotatingMinTransmissions, 7);
  assert.equal(overridden.rotatingMachineNameShare, 0.5);
});
