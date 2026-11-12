import { Counter, Gauge, Histogram } from 'prom-client';
import { metricsRegistry } from './registry.js';

export const mqttMessagesTotal = new Counter({
  name: 'meshcore_mqtt_messages_total',
  help: 'MQTT messages accepted by the ingest handler.',
  labelNames: ['network'] as const,
  registers: [metricsRegistry],
});

export const mqttDeliveryProbeHealthy = new Gauge({
  name: 'meshcore_mqtt_delivery_probe_healthy',
  help: 'Whether the ingest session has successfully received its QoS 1 delivery probe.',
  registers: [metricsRegistry],
});

export const mqttDeliveryProbeEnabled = new Gauge({
  name: 'meshcore_mqtt_delivery_probe_enabled',
  help: 'Whether this process owns an active MQTT ingest delivery probe.',
  registers: [metricsRegistry],
});

export const mqttDeliveryProbeLastSuccess = new Gauge({
  name: 'meshcore_mqtt_delivery_probe_last_success_timestamp_seconds',
  help: 'Last successful broker-to-ingest QoS 1 delivery probe.',
  registers: [metricsRegistry],
});

export const mqttDeliveryProbeRecoveries = new Counter({
  name: 'meshcore_mqtt_delivery_probe_recoveries_total',
  help: 'Persistent ingest session reconnects after a QoS 1 delivery probe timeout.',
  registers: [metricsRegistry],
});

export const mqttDeniedPublishesTotal = new Counter({
  name: 'meshcore_mqtt_denied_publishes_total',
  help: 'Denied PUBLISH broker-log observations by bounded topic suffix and scan phase; history may replay at startup.',
  labelNames: ['kind', 'phase'] as const,
  registers: [metricsRegistry],
});

export const mqttIngestOutcomesTotal = new Counter({
  name: 'meshcore_mqtt_ingest_outcomes_total',
  help: 'MQTT ingest messages by durable handler outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const mqttIngestActive = new Gauge({
  name: 'meshcore_mqtt_ingest_active',
  help: 'MQTT messages currently being processed before acknowledgement.',
  registers: [metricsRegistry],
});

export const mqttIngestQueueDepth = new Gauge({
  name: 'meshcore_mqtt_ingest_queue_depth',
  help: 'Received MQTT messages awaiting a durable outcome, including active writes.',
  registers: [metricsRegistry],
});

export const mqttConnectionMonitorHealthy = new Gauge({
  name: 'meshcore_mqtt_connection_monitor_healthy',
  help: 'Whether the broker-log monitor has completed a read without outstanding read or audit-write failures.',
  registers: [metricsRegistry],
});

export const mqttConnectionMonitorClients = new Gauge({
  name: 'meshcore_mqtt_connection_monitor_clients',
  help: 'Retained bounded broker client correlations; log observations never grant ownership.',
  registers: [metricsRegistry],
});

export const mqttConnectionMonitorErrorsTotal = new Counter({
  name: 'meshcore_mqtt_connection_monitor_errors_total',
  help: 'Broker-log monitor failures by bounded phase.',
  labelNames: ['phase'] as const,
  registers: [metricsRegistry],
});

export const packetBatchFlushTotal = new Counter({
  name: 'meshcore_packet_batch_flush_total',
  help: 'Atomic packet batch flushes by bounded outcome.',
  labelNames: ['outcome'] as const,
  registers: [metricsRegistry],
});

export const packetBatchRetryTotal = new Counter({
  name: 'meshcore_packet_batch_retries_total',
  help: 'Transient database retry attempts for packet batches.',
  registers: [metricsRegistry],
});

export const packetBatchWriterContentionTotal = new Counter({
  name: 'meshcore_packet_batch_writer_contention_total',
  help: 'Attempts rejected because another packet-batch writer held the exclusive writer lock.',
  registers: [metricsRegistry],
});

export const packetBatchSize = new Histogram({
  name: 'meshcore_packet_batch_size',
  help: 'Packets committed per atomic batch.',
  buckets: [1, 2, 5, 10, 20, 35, 50],
  registers: [metricsRegistry],
});

export const packetBatchDuration = new Histogram({
  name: 'meshcore_packet_batch_duration_seconds',
  help: 'Atomic packet batch flush duration.',
  labelNames: ['outcome'] as const,
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15],
  registers: [metricsRegistry],
});

export const packetBatchQueueDepth = new Gauge({
  name: 'meshcore_packet_batch_queue_depth',
  help: 'Packet batch entries waiting to be persisted.',
  registers: [metricsRegistry],
});

export const packetBatchQueueWait = new Histogram({
  name: 'meshcore_packet_batch_queue_wait_seconds',
  help: 'Time from packet batch enqueue to commit.',
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15],
  registers: [metricsRegistry],
});

export const packetBatchDedupeTotal = new Counter({
  name: 'meshcore_packet_batch_dedupe_total',
  help: 'Duplicate packets collapsed inside a batch before persistence.',
  registers: [metricsRegistry],
});

export const mqttIngestAckDuration = new Histogram({
  name: 'meshcore_mqtt_ingest_ack_duration_seconds',
  help: 'MQTT processing time from dispatch to outcome; excludes intake waiting and wire acknowledgement.',
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15, 30],
  registers: [metricsRegistry],
});

export const mqttIntakeQueueWait = new Histogram({
  name: 'meshcore_mqtt_intake_queue_wait_seconds',
  help: 'Time from MQTT intake admission to persistence dispatch, including per-topic waiting.',
  buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 30, 60, 120, 300],
  registers: [metricsRegistry],
});

export const mqttWireAckDuration = new Histogram({
  name: 'meshcore_mqtt_wire_ack_duration_seconds',
  help: 'Time from intake admission to successful QoS 1 PUBACK write, including queue and commit waiting.',
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15, 30, 60, 120, 300],
  registers: [metricsRegistry],
});

export const postPersistInFlightGauge = new Gauge({
  name: 'meshcore_mqtt_post_persist_inflight',
  help: 'Best-effort post-persist follow-ups currently in flight (bounded).',
  registers: [metricsRegistry],
});

export const mqttDedupEntries = new Gauge({
  name: 'meshcore_mqtt_dedup_entries',
  help: 'Committed observation identities retained in the bounded 50000-entry local dedup map.',
  registers: [metricsRegistry],
});

export const mqttPreResolveHistoryEntries = new Gauge({
  name: 'meshcore_mqtt_preresolve_history_entries',
  help: 'Cooldown timestamps retained in the bounded 50000-entry pre-resolve history.',
  registers: [metricsRegistry],
});

export const mqttPreResolveInflight = new Gauge({
  name: 'meshcore_mqtt_preresolve_inflight',
  help: 'Pre-resolve requests awaiting the bounded background resolver pool.',
  registers: [metricsRegistry],
});

export const postPersistDroppedTotal = new Counter({
  name: 'meshcore_mqtt_post_persist_dropped_total',
  help: 'Best-effort post-persist follow-ups dropped because the in-flight bound was reached.',
  registers: [metricsRegistry],
});

// ---------------------------------------------------------------- RAM ingest
