import type { DeliveryHealth } from '../mqtt/deliveryProbe.js';

export function withMqttDeliveryHealth<T extends {
  status: string;
  incidents: Array<{ code: string; severity: string }>;
  components: { ingest: { status: string } };
}>(overview: T, delivery?: DeliveryHealth): T {
  if (delivery?.state !== 'stalled') return overview;
  return { ...overview, status: 'critical',
    incidents: [...overview.incidents, { code: 'mqtt_delivery_stalled', severity: 'critical' }],
    components: { ...overview.components, ingest: { status: 'stalled' } } };
}
