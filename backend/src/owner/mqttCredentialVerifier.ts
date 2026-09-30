export type BrokerCredentialCheck = (mqttUsername: string, mqttPassword: string) => Promise<boolean>;

/** Verify each login against the broker so cached success cannot outlive a password reset. */
export function createMqttCredentialVerifier(check: BrokerCredentialCheck): BrokerCredentialCheck {
  return (mqttUsername, mqttPassword) => check(mqttUsername, mqttPassword);
}
