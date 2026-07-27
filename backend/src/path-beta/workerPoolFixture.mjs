import { parentPort } from 'node:worker_threads';

parentPort.on('message', (message) => {
  const delayMs = Number(message.delayMs ?? 0);
  setTimeout(() => {
    parentPort.postMessage({
      id: message.id,
      ok: true,
      result: message.value ?? null,
    });
  }, delayMs);
});
