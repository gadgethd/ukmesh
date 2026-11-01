import mqtt, { type IClientOptions, type MqttClient } from 'mqtt';

export function connectPersistentMqtt(url: string, options: IClientOptions): MqttClient {
  // We subscribe explicitly after every CONNACK. Disable MQTT.js's subscription
  // cache, which otherwise returns [] instead of sending another SUBSCRIBE.
  return mqtt.connect(url, { ...options, clean: false, resubscribe: false });
}

export function subscribePersistentTopics(client: MqttClient, topics: string[], callbacks: {
  ready: () => void;
  failed: (error: Error) => void;
  subscribed?: (topic: string) => void;
}): void {
  let remaining = topics.length;
  for (const topic of topics) {
    client.subscribe(topic, { qos: 1 }, (error, granted) => {
      if (error || !granted?.length || granted.some(entry => entry.qos === 128)) {
        callbacks.failed(error ?? new Error('MQTT_SUBSCRIPTION_DENIED'));
        return;
      }
      callbacks.subscribed?.(topic);
      if (--remaining === 0) callbacks.ready();
    });
  }
}
