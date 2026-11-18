import type { MqttClient } from 'mqtt';
import { mqttBrokerStore } from '../metrics/ingest.js';

const storeTopics = new Map([
  ['$SYS/broker/store/messages/count', 'messages'],
  ['$SYS/broker/store/messages/bytes', 'bytes'],
  ['$SYS/broker/publish/messages/dropped', 'dropped'],
]);

/** Optional telemetry uses a separate subscription and never gates intake readiness. */
export function createBrokerBacklogMonitor(client: Pick<MqttClient, 'subscribe'>) {
  return {
    subscribe() {
      client.subscribe([...storeTopics.keys()], { qos: 0 }, (error, granted) => {
        if (error || granted?.length !== storeTopics.size || granted.some(item => item.qos === 128)) {
          console.warn('[mqtt] broker backlog metrics unavailable');
        }
      });
    },
    accept(packet: { topic: string; payload: Buffer | string; qos?: number }): boolean {
      const kind = storeTopics.get(packet.topic);
      if (!kind) return false;
      const payload = packet.payload.toString();
      const value = Number(payload);
      if (packet.qos === 0 && /^\d+$/.test(payload) && Number.isSafeInteger(value)) {
        mqttBrokerStore.set({ kind }, value);
      }
      return true;
    },
  };
}
