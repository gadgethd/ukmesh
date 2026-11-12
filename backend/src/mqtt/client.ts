import { createPacketProcessor } from '../ingest/packetProcessor.js';
import type { IngestEvents } from '../ingest/events.js';
import type { CommittedFollowUps } from '../ingest/committedFollowUps.js';
import { boundedEnvInteger } from '../platform/config/boundedNumber.js';
import type { MqttClient } from 'mqtt';
import { closePacketBatch, flush as flushPacketBatch } from '../db/packetBatch.js';
import { initSpamDetector } from './spamDetector.js';
import { installDurableMqttIntake } from './durableIntake.js';
import { createDeliveryProbe, type DeliveryHealth } from './deliveryProbe.js';
import { connectPersistentMqtt, subscribePersistentTopics } from './persistentClient.js';
import { assertSupportedIngestConnections, loadMqttIntakeConfig } from './ingestConfig.js';
import { waitForShutdownStep } from './shutdownDeadline.js';
import { mqttIntakeQueueWait, mqttWireAckDuration, mqttIngestQueueDepth, mqttIngestOutcomesTotal, mqttDeliveryProbeEnabled, mqttDeliveryProbeHealthy, mqttDeliveryProbeLastSuccess, mqttDeliveryProbeRecoveries } from '../metrics/ingest.js';

export type MqttRuntimeStatus = {
  state: 'starting' | 'connected' | 'disconnected' | 'reconnecting' | 'error';
  changedAt: string;
  lastError?: string;
  delivery?: DeliveryHealth;
};

export function createMqttRuntime(deps: { events: IngestEvents; committedFollowUps: CommittedFollowUps }) {
  const MQTT_CLIENT_ID = String(
    process.env['MQTT_CLIENT_ID'] ?? 'meshcore-analytics-ingest',
  ).trim() || 'meshcore-analytics-ingest';
  const events = deps.events;
  const processor = createPacketProcessor({ clientId: MQTT_CLIENT_ID, events, committedFollowUps: deps.committedFollowUps, markMqttPersistenceHealthy });
  const TOPIC_PREFIXES = processor.topicPrefixes;
  const MQTT_MAX_PAYLOAD_BYTES = processor.maxPayloadBytes;
  let mqttRuntimeStatus: MqttRuntimeStatus = { state: 'starting', changedAt: new Date().toISOString() };
  const mqttClients: MqttClient[] = [];
  const durableIntakes: ReturnType<typeof installDurableMqttIntake>[] = [];
  const deliveryProbes: Array<{ probe: ReturnType<typeof createDeliveryProbe>; timer: NodeJS.Timeout }> = [];
  let deliveryHealth: DeliveryHealth = { state: 'disabled', lastSuccessAt: null, recoveries: 0 };
  let mqttStopping = false;
  let mqttDurabilityFailure: string | undefined;

  function setMqttRuntimeStatus(state: MqttRuntimeStatus['state'], lastError?: string): void {
    mqttRuntimeStatus = {
      state,
      changedAt: new Date().toISOString(),
      ...(lastError ? { lastError: lastError.slice(0, 200) } : {}),
    };
  }

  function getMqttRuntimeStatus(): MqttRuntimeStatus {
    const lastError = deliveryHealth.state === 'stalled' ? 'MQTT_QOS1_DELIVERY_STALLED' : mqttDurabilityFailure;
    return { ...mqttRuntimeStatus, delivery: { ...deliveryHealth },
      ...(lastError ? { state: 'error' as const, lastError } : {}) };
  }

  function markMqttPersistenceHealthy(): void {
    mqttDurabilityFailure = undefined;
  }

  function markMqttPersistenceFailed(error: unknown): void {
    mqttDurabilityFailure = (error instanceof Error ? error.message : String(error)).slice(0, 200);
  }

  /**
   * The default and only qualified topology is one durable connection. The
   * configured value is checked before connecting; shared sessions need a
   * successful restart and resize qualification before they can be enabled.
   */
  const MQTT_INGEST_CONNECTIONS = boundedEnvInteger('MQTT_INGEST_CONNECTIONS', 1, 1, 16);
  const MQTT_INTAKE_CONFIG = loadMqttIntakeConfig(process.env);
  const MQTT_INGEST_SHARE_GROUP = String(
    process.env['MQTT_INGEST_SHARE_GROUP'] ?? 'meshcore-ingest',
  ).trim() || 'meshcore-ingest';

  function reportMqttIngestFailure(client: MqttClient, error: Error): void {
    mqttIngestOutcomesTotal.inc({ outcome: 'failure' });
    mqttDurabilityFailure = error.message.slice(0, 200) || 'MQTT persistence failed';
    console.error('[mqtt] durable ingest failed; leaving delivery unacknowledged:', error.message);
    // MQTT.js forwards handleMessage errors to its parser callback, which does
    // not send PUBACK. Close the stream so the persistent broker session
    // redelivers the unacknowledged QoS 1 message instead of leaving it inflight.
    if (!mqttStopping && client.connected) client.stream.destroy();
  }


  /** Publish through the authenticated ingest connection on the internal broker. */
  async function publishMqtt(topic: string, payload: string): Promise<void> {
    const client = mqttClients.find((candidate) => candidate.connected);
    if (!client) throw new Error("MQTT_PUBLISH_NOT_CONNECTED");
    await client.publishAsync(topic, payload, { qos: 0, retain: false });
  }

  async function startMqttClient(): Promise<void> {
    if (mqttStopping) return;
    processor.start();
    assertSupportedIngestConnections(MQTT_INGEST_CONNECTIONS);
    // Must complete before connecting to MQTT — the broker replays buffered
    // messages immediately on connect, and knownNodeIds must be populated first.
    await initSpamDetector();
    if (mqttStopping) return;

    const brokerUrl = process.env['MQTT_BROKER_URL'] ?? 'ws://mosquitto:9001';
    const redactedUrl = brokerUrl.replace(/\/\/[^@]*@/, '//***:***@');
    console.log(`[mqtt] connecting to ${redactedUrl}`);
    console.log(`[mqtt] channels: ${processor.channelNames.join(', ')}`);
    console.log(`[mqtt] topic prefixes: ${Array.from(TOPIC_PREFIXES).join(', ')}`);
    console.log(`[mqtt] durable ingest maxPayload=${MQTT_MAX_PAYLOAD_BYTES}`);
    console.log(`[mqtt] persistent session clientId=${MQTT_CLIENT_ID} subscriptionQos=1`);

    console.log(`[mqtt] connections=${MQTT_INGEST_CONNECTIONS}`);
    for (let index = 0; index < MQTT_INGEST_CONNECTIONS; index += 1) {
      const connectionLabel = MQTT_INGEST_CONNECTIONS > 1
        ? ` (connection ${index + 1}/${MQTT_INGEST_CONNECTIONS})`
        : '';
      const client = connectPersistentMqtt(brokerUrl, {
        reconnectPeriod: 5000,
        connectTimeout: 30000,
        clientId: MQTT_INGEST_CONNECTIONS > 1 ? `${MQTT_CLIENT_ID}-${index + 1}` : MQTT_CLIENT_ID,
        username: process.env['MQTT_USERNAME'],
        password: process.env['MQTT_PASSWORD'],
      });
      mqttClients.push(client);
      mqttDeliveryProbeEnabled.set(1);
      let previousRecoveries = 0;
      const probeTopic = `${Array.from(TOPIC_PREFIXES)[0]}/_system/ingest/${MQTT_CLIENT_ID}`;
      const probe = createDeliveryProbe({
        topic: probeTopic,
        publish: (nonce) => {
          client.publish(probeTopic, nonce, { qos: 1, retain: false }, (error) => {
            if (error && !mqttStopping) console.error('[mqtt] delivery probe publish failed:', error.message);
          });
        },
        reconnect: () => {
          console.error('[mqtt] QoS 1 delivery stalled; reconnecting the persistent session');
          if (!mqttStopping && client.connected) client.stream.destroy();
        },
        changed: (health) => {
          deliveryHealth = health;
          mqttDeliveryProbeHealthy.set(health.state === 'healthy' ? 1 : 0);
          if (health.lastSuccessAt) mqttDeliveryProbeLastSuccess.set(Date.parse(health.lastSuccessAt) / 1000);
          if (health.recoveries > previousRecoveries) mqttDeliveryProbeRecoveries.inc(health.recoveries - previousRecoveries);
          previousRecoveries = health.recoveries;
        },
      });
      deliveryHealth = probe.status();
      mqttDeliveryProbeHealthy.set(0);
      const probeTimer = setInterval(probe.tick, 5_000);
      probeTimer.unref();
      deliveryProbes.push({ probe, timer: probeTimer });
      durableIntakes.push(installDurableMqttIntake(client,
        (packet) => probe.accept(packet) ? Promise.resolve() : processor.process(packet),
        (error) => reportMqttIngestFailure(client, error),
        {
          ...MQTT_INTAKE_CONFIG,
          depth: (count) => mqttIngestQueueDepth.set(count),
          observeQueueWait: (seconds) => mqttIntakeQueueWait.observe(seconds),
          observeWireAck: (seconds) => mqttWireAckDuration.observe(seconds),
        },
      ));

      client.on('connect', () => {
        setMqttRuntimeStatus('connected');
        console.log(`[mqtt] connected${connectionLabel}`);
        const topicFilters = Array.from(TOPIC_PREFIXES, prefix => {
          // A shared subscription spreads messages across the ingest
          // connections; a single connection keeps the historical direct
          // subscription and its topic-ACL behaviour exactly.
          return MQTT_INGEST_CONNECTIONS > 1
            ? `$share/${MQTT_INGEST_SHARE_GROUP}/${prefix}/#`
            : `${prefix}/#`;
        });
        subscribePersistentTopics(client, topicFilters, {
          ready: probe.connect,
          failed: error => reportMqttIngestFailure(client, error),
          subscribed: topic => console.log(`[mqtt] subscribed to ${topic}${connectionLabel}`),
        });
      });

      client.on('error', (err) => {
        setMqttRuntimeStatus('error', err.message);
        console.error('[mqtt] error', err.message);
      });
      client.on('offline', () => {
        if (!mqttClients.some((candidate) => candidate.connected)) {
          setMqttRuntimeStatus('disconnected');
        }
      });
      client.on('close', () => {
        probe.disconnect();
        if (!mqttClients.some((candidate) => candidate.connected)) {
          setMqttRuntimeStatus('disconnected');
        }
        void flushPacketBatch().catch((err: unknown) => {
          console.error('[mqtt] packet batch close flush failed:', (err as Error).message);
        });
      });
      client.on('reconnect', () => {
        setMqttRuntimeStatus('reconnecting');
        console.log(`[mqtt] reconnecting…${connectionLabel}`);
      });
    }
  }

  async function stopMqttClient(timeoutMs = 20_000): Promise<void> {
    mqttStopping = true;
    for (const { probe, timer } of deliveryProbes.splice(0)) { clearInterval(timer); probe.disconnect(); }
    mqttDeliveryProbeEnabled.set(0);
    const deadline = Date.now() + timeoutMs;
    for (const intake of durableIntakes) intake.stopAdmission();
    const clients = mqttClients.splice(0, mqttClients.length);
    for (const client of clients) {
      client.removeAllListeners('reconnect');
      await waitForShutdownStep(client.endAsync(false), deadline, 'disconnect').catch((error: unknown) => {
        if (error instanceof Error && error.message.startsWith('MQTT_DRAIN_TIMEOUT:')) throw error;
        console.error('[mqtt] disconnect failed:', error instanceof Error ? error.message : error);
      });
    }
    await waitForShutdownStep(Promise.all(durableIntakes.map((intake) => intake.drain())), deadline, 'intake');
    durableIntakes.length = 0;
    while (processor.activeCount() > 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    if (processor.activeCount() > 0) {
      throw new Error(
        `MQTT_DRAIN_TIMEOUT:active=${processor.activeCount()}`,
      );
    }
    events.flushNodes();
    await waitForShutdownStep(closePacketBatch(), deadline, 'batch');
    await waitForShutdownStep(processor.close(), deadline, 'public-live');
    setMqttRuntimeStatus('disconnected');
  }

  return { startMqttClient, stopMqttClient, getMqttRuntimeStatus, publishMqtt, markMqttPersistenceHealthy, markMqttPersistenceFailed };
}
export type MqttRuntime = ReturnType<typeof createMqttRuntime>;
