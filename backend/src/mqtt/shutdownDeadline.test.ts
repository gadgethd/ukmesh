import assert from 'node:assert/strict';
import test from 'node:test';
import { waitForShutdownStep } from './shutdownDeadline.js';

test('F15 one MQTT shutdown deadline includes a blocked disconnect', async () => {
  const never = new Promise<void>(() => {});
  const deadline = Date.now() + 20;
  await assert.rejects(waitForShutdownStep(never, deadline, 'disconnect'), /MQTT_DRAIN_TIMEOUT:disconnect/);
  await assert.rejects(waitForShutdownStep(Promise.resolve(), deadline - 1, 'batch'), /MQTT_DRAIN_TIMEOUT:batch/);
});
