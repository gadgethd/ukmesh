import assert from 'node:assert/strict';
import test from 'node:test';
import { createDurableMqttHandleMessage } from './durableHandler.js';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

test('MQTT acknowledgement callback waits for durable processing', async () => {
  const commit = deferred<void>();
  const ack = deferred<Error | null | undefined>();
  let persisted = false;
  let ackCalled = false;
  const handle = createDurableMqttHandleMessage(async (packet) => {
    assert.equal(packet.topic, 'meshcore/MME/observer/packets');
    assert.equal(packet.payload.toString(), '{"raw":"01"}');
    await commit.promise;
    persisted = true;
  }, () => assert.fail('successful persistence must not report failure'));

  handle({
    topic: 'meshcore/MME/observer/packets',
    payload: Buffer.from('{"raw":"01"}'),
  }, (error) => {
    ackCalled = true;
    ack.resolve(error);
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(persisted, false);
  assert.equal(ackCalled, false);

  commit.resolve();
  assert.equal(await ack.promise, undefined);
  assert.equal(ackCalled, true);
  assert.equal(persisted, true);
});

test('persistence failure is reported to MQTT.js without a success acknowledgement', async () => {
  const failed = new Error('database unavailable');
  const ack = deferred<Error | null | undefined>();
  let reported: Error | undefined;
  const handle = createDurableMqttHandleMessage(async () => {
    throw failed;
  }, (error) => {
    reported = error;
  });

  handle({ topic: 'meshcore/MME/observer/status', payload: Buffer.from('{}') }, (error) => {
    ack.resolve(error);
  });

  assert.equal(await ack.promise, failed);
  assert.equal(reported, failed);
});

test('synchronous persistence errors also reach the MQTT acknowledgement callback', async () => {
  const failure = new Error('bad input');
  const ack = deferred<Error | null | undefined>();
  const handle = createDurableMqttHandleMessage(() => {
    throw failure;
  }, () => {});

  handle({ topic: 'meshcore/MME/observer/neighbors', payload: Buffer.from('{}') }, (error) => {
    ack.resolve(error);
  });

  assert.equal(await ack.promise, failure);
});
