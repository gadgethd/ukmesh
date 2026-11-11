import { BoundedTtlMap } from '../cache/boundedTtlMap.js';

export type MqttOnboardingDiagnostic = {
  code: 'MQTT_DEFAULT_TOPIC_ROOT';
  message: string;
  observedAt: string;
};

export const MQTT_ONBOARDING_DIAGNOSTIC_TTL_MS = 15 * 60_000;

// Diagnostic hints only. A client ID or a claimed topic never proves ownership.
export function createMqttOnboardingDiagnostics(now = Date.now, maxEntries = 4_096) {
  const recent = new BoundedTtlMap<string, MqttOnboardingDiagnostic>({
    name: 'mqtt_onboarding', maxEntries, maxWeight: 4 * 1024 * 1024,
    ttlMs: MQTT_ONBOARDING_DIAGNOSTIC_TTL_MS, now,
  });

  function record(mqttUsername: string, topic: string, observedAt: number): void {
    if (!/^[A-Za-z0-9_.@-]{1,128}$/.test(mqttUsername)) return;
    if (!/^meshcore(?:\/(?:packets|status|neighbors|neighbours))?$/.test(topic)) return;
    const age = now() - observedAt;
    // Historical scans must not turn old denials into fresh user warnings.
    if (!Number.isFinite(age) || age < 0 || age >= MQTT_ONBOARDING_DIAGNOSTIC_TTL_MS) return;
    const previous = recent.get(mqttUsername);
    if (previous && Date.parse(previous.observedAt) > observedAt) return;
    recent.set(mqttUsername, {
      code: 'MQTT_DEFAULT_TOPIC_ROOT',
      message: `The broker denied a publish to "${topic}" because the firmware default topic root lacks your IATA code and node public key. In the flasher MQTT settings, set Topic root to meshcore/{IATA}/{PUBLIC_KEY}/packets and set IATA to your location code, then apply the settings. Keep the braces: the firmware fills in both values. If the full topic is still denied, ask sudogadget to verify the owner grant for your full public key.`,
      observedAt: new Date(observedAt).toISOString(),
    });
  }

  function get(mqttUsername: string): MqttOnboardingDiagnostic | null {
    const diagnostic = recent.get(mqttUsername);
    if (!diagnostic) return null;
    if (now() - Date.parse(diagnostic.observedAt) >= MQTT_ONBOARDING_DIAGNOSTIC_TTL_MS) {
      recent.delete(mqttUsername);
      return null;
    }
    return diagnostic;
  }

  return { record, get, close: () => recent.shutdown() };
}

export const mqttOnboardingDiagnostics = createMqttOnboardingDiagnostics();
