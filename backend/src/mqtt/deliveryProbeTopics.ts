/**
 * Delivery controls may use a separate namespace without changing the business
 * subscription or its external broker ACL contract. The unset default retains
 * the historical topic and requires no additional subscription permission.
 */
export function deliveryProbeTopics(
  businessPrefixes: ReadonlySet<string>,
  clientId: string,
  configuredPrefix?: string,
): { topic: string; subscription?: string } {
  const firstPrefix = businessPrefixes.values().next().value;
  if (!firstPrefix) throw new Error('MQTT_TOPIC_PREFIXES_EMPTY');
  const explicitPrefix = configuredPrefix?.trim();
  if (explicitPrefix && (/[+#\0]/.test(explicitPrefix)
    || explicitPrefix.split('/').some(part => part.length === 0))) {
    throw new Error('MQTT_DELIVERY_PROBE_TOPIC_PREFIX_INVALID');
  }
  const topic = `${explicitPrefix || firstPrefix}/_system/ingest/${clientId}`;
  if (/[+#\0]/.test(topic)) throw new Error('MQTT_DELIVERY_PROBE_TOPIC_INVALID');
  const alreadyCovered = Array.from(businessPrefixes).some(prefix => topic.startsWith(`${prefix}/`));
  return { topic, ...(!alreadyCovered ? { subscription: topic } : {}) };
}
