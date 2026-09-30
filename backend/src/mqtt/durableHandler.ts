export type DurableMqttPacket = {
  topic: string;
  payload: Buffer | string;
};

export type MqttHandleMessageCallback = (error?: Error) => void;

/**
 * MQTT.js calls handleMessage before sending PUBACK for QoS 1. Calling its
 * callback only after persistence gives the broker backpressure and lets it
 * redeliver an unacknowledged message after a disconnect.
 */
export function createDurableMqttHandleMessage(
  persist: (packet: DurableMqttPacket) => Promise<void>,
  onFailure: (error: Error) => void,
): (packet: DurableMqttPacket, callback: MqttHandleMessageCallback) => void {
  return (packet, callback) => {
    void Promise.resolve()
      .then(() => persist(packet))
      .then(
        () => callback(),
        (cause: unknown) => {
          const error = cause instanceof Error ? cause : new Error(String(cause));
          try {
            onFailure(error);
          } finally {
            callback(error);
          }
        },
      );
  };
}
