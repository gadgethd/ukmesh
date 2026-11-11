import { randomUUID } from 'node:crypto';

export type DeliveryHealth = {
  state: 'disabled' | 'starting' | 'healthy' | 'stalled';
  lastSuccessAt: string | null;
  recoveries: number;
};

/** Probe the broker-to-subscriber QoS 1 path, independently of flowing QoS 0 traffic. */
export function createDeliveryProbe(options: {
  topic: string;
  publish: (nonce: string) => void;
  reconnect: () => void;
  changed: (health: DeliveryHealth) => void;
  now?: () => number;
  nonce?: () => string;
  intervalMs?: number;
  timeoutMs?: number;
  recoveryCooldownMs?: number;
}) {
  const now = options.now ?? Date.now;
  let connected = false;
  let pending: { nonce: string; deadline: number } | null = null;
  let nextProbeAt = 0;
  let nextRecoveryAt = 0;
  let health: DeliveryHealth = { state: 'starting', lastSuccessAt: null, recoveries: 0 };
  const change = (state: DeliveryHealth['state']) => {
    health = { ...health, state };
    options.changed({ ...health });
  };
  const tick = () => {
    if (!connected) return;
    const time = now();
    if (pending && time >= pending.deadline) {
      change('stalled');
      if (time >= nextRecoveryAt) {
        nextRecoveryAt = time + (options.recoveryCooldownMs ?? 300_000);
        health.recoveries += 1;
        options.changed({ ...health });
        options.reconnect();
      }
      return;
    }
    if (!pending && time >= nextProbeAt) {
      pending = { nonce: (options.nonce ?? randomUUID)(), deadline: time + (options.timeoutMs ?? 60_000) };
      nextProbeAt = time + (options.intervalMs ?? 30_000);
      options.publish(pending.nonce);
    }
  };
  return {
    connect() { connected = true; pending = null; nextProbeAt = 0; tick(); },
    disconnect() { connected = false; pending = null; },
    tick,
    accept(packet: { topic: string; payload: Buffer | string; qos?: number; retain?: boolean }) {
      if (packet.topic !== options.topic) return false;
      // Retained, forged, or late probes cannot certify the current session.
      if (connected && pending && !packet.retain && packet.qos === 1 && packet.payload.toString() === pending.nonce) {
        pending = null;
        health.lastSuccessAt = new Date(now()).toISOString();
        change('healthy');
      }
      return true;
    },
    status: () => ({ ...health }),
  };
}
