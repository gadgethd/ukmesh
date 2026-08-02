import { FormEvent, ReactNode, useEffect, useState } from 'react';
import { LoadingIndicator } from '../LoadingIndicator.js';

export function OwnerLoginSection(props: {
  username: string;
  password: string;
  submitting: boolean;
  error: string | null;
  onUsername: (value: string) => void;
  onPassword: (value: string) => void;
  onSubmit: (event: FormEvent) => void;
}) {
  return (
    <section className="prose-section owner-login">
      <h2>Login</h2>
      <p className="prose-note">Enter the MQTT credentials associated with your repeater observer.</p>
      <form className="owner-login__form" onSubmit={props.onSubmit}>
        <label className="owner-login__label" htmlFor="owner-username">MQTT username</label>
        <input id="owner-username" className="owner-login__input" autoComplete="username" value={props.username} onChange={(event) => props.onUsername(event.target.value)} maxLength={128} />
        <label className="owner-login__label" htmlFor="owner-key">MQTT password</label>
        <input id="owner-key" className="owner-login__input" type="password" autoComplete="current-password" value={props.password} onChange={(event) => props.onPassword(event.target.value)} maxLength={256} />
        <button className="site-btn site-btn--primary owner-login__button" type="submit" disabled={props.submitting}>
          {props.submitting ? <LoadingIndicator label="Logging in..." variant="inline" /> : 'Login'}
        </button>
      </form>
      {props.error && <p className="prose-note owner-login__error">{props.error}</p>}
    </section>
  );
}

export function OwnerSection({ children }: { children: ReactNode }) {
  return <>{children}</>;
}

type Rule = {
  id: string;
  node_id: string;
  rule_type: string;
  threshold: number;
  enabled: boolean;
  pause_reason: string | null;
  last_delivery_success_at: string | null;
  last_delivery_error_at: string | null;
  last_delivery_error: string | null;
  destination: { configured: boolean; host: string | null };
};
type DeliveryAttempt = {
  attempt: number;
  outcome: string;
  httpStatus: number | null;
  error: string | null;
  startedAt: string;
  completedAt: string;
};
type Delivery = {
  id: string;
  rule_id: string;
  node_id: string;
  rule_type: string;
  destination_host: string;
  status: string;
  attempts: number;
  is_test: boolean;
  next_attempt_at: string;
  delivered_at: string | null;
  last_error: string | null;
  created_at: string;
  attempt_history: DeliveryAttempt[];
};
async function ownerCsrf(): Promise<string> {
  const response = await fetch('/api/owner/csrf', { cache: 'no-store' });
  const value = await response.json() as { csrfToken?: string };
  if (!response.ok || !value.csrfToken) throw new Error('Could not prepare secure request');
  return value.csrfToken;
}

export function OwnerAlertSettings({ nodes, selectedNodeId }: { nodes: Array<{ node_id: string; name?: string | null }>; selectedNodeId: string }) {
  const [rules, setRules] = useState<Rule[]>([]);
  const [deliveries, setDeliveries] = useState<Delivery[]>([]);
  const [ruleType, setRuleType] = useState('offline_minutes');
  const [threshold, setThreshold] = useState('30');
  const [webhook, setWebhook] = useState('');
  const [message, setMessage] = useState<string | null>(null);
  const [busyRuleId, setBusyRuleId] = useState<string | null>(null);
  const load = async () => {
    const [ruleResponse, deliveryResponse] = await Promise.all([
      fetch('/api/owner/alert-rules', { cache: 'no-store' }),
      fetch('/api/owner/alert-deliveries', { cache: 'no-store' }),
    ]);
    if (!ruleResponse.ok || !deliveryResponse.ok) throw new Error('Could not load alert delivery state');
    const nextRules = await ruleResponse.json() as Rule[];
    const history = await deliveryResponse.json() as { deliveries?: Delivery[] };
    setRules(Array.isArray(nextRules) ? nextRules : []);
    setDeliveries(Array.isArray(history.deliveries) ? history.deliveries : []);
  };
  useEffect(() => {
    void load().catch((error: Error) => setMessage(error.message));
    const timer = window.setInterval(() => {
      if (document.visibilityState === 'visible') void load().catch(() => {});
    }, 15_000);
    return () => window.clearInterval(timer);
  }, []);
  const submit = (event: FormEvent) => {
    event.preventDefault();
    ownerCsrf().then((csrfToken) => fetch('/api/owner/alert-rules', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrfToken },
      body: JSON.stringify({ nodeId: selectedNodeId, ruleType, threshold: Number(threshold), webhook, enabled: true }),
    })).then(async (response) => {
      const value = await response.json().catch(() => ({})) as { error?: string };
      if (!response.ok) throw new Error(value.error ?? `HTTP ${response.status}`);
      setMessage('Alert rule saved.');
      void load();
    }).catch((error: Error) => setMessage(error.message));
  };
  const sendTest = async (ruleId: string) => {
    setBusyRuleId(ruleId);
    setMessage(null);
    try {
      const csrfToken = await ownerCsrf();
      const response = await fetch(`/api/owner/alert-rules/${ruleId}/test`, {
        method: 'POST',
        headers: {
          'X-CSRF-Token': csrfToken,
          'Idempotency-Key': crypto.randomUUID(),
        },
      });
      const value = await response.json().catch(() => ({})) as { error?: string; status?: string };
      if (!response.ok) throw new Error(value.error ?? `HTTP ${response.status}`);
      setMessage(value.status === 'already_queued'
        ? 'That test request was already queued.'
        : 'Test delivery queued. Its result will appear below.');
      await load();
    } catch (error) {
      setMessage((error as Error).message);
    } finally {
      setBusyRuleId(null);
    }
  };
  const removeRule = async (ruleId: string) => {
    setBusyRuleId(ruleId);
    setMessage(null);
    try {
      const csrfToken = await ownerCsrf();
      const response = await fetch(`/api/owner/alert-rules/${ruleId}`, {
        method: 'DELETE',
        headers: { 'X-CSRF-Token': csrfToken },
      });
      if (!response.ok) throw new Error(`Could not remove rule (HTTP ${response.status})`);
      setMessage('Alert rule removed.');
      await load();
    } catch (error) {
      setMessage((error as Error).message);
    } finally {
      setBusyRuleId(null);
    }
  };
  return (
    <section className="prose-section owner-settings">
      <h2>Alert settings</h2>
      <p>Create per-node health rules. Delivery is idempotent, retried up to five times, and paused after a terminal failure.</p>
      <form className="owner-settings__form" onSubmit={submit}>
        <label>Node<select value={selectedNodeId} disabled>{nodes.map((node) => <option key={node.node_id} value={node.node_id}>{node.name ?? node.node_id}</option>)}</select></label>
        <label>Condition<select value={ruleType} onChange={(event) => setRuleType(event.target.value)}><option value="offline_minutes">Offline minutes</option><option value="battery_below_mv">Battery below mV</option><option value="link_loss_above_db">Path loss above dB</option></select></label>
        <label>Threshold<input type="number" min="1" value={threshold} onChange={(event) => setThreshold(event.target.value)} /></label>
        <label>HTTPS webhook (optional)<input type="url" value={webhook} onChange={(event) => setWebhook(event.target.value)} /></label>
        <button type="submit" className="site-btn site-btn--primary">Save rule</button>
      </form>
      {message && <p role="status" aria-live="polite">{message}</p>}
      <div className="owner-settings__rules">
        {rules.map((rule) => (
          <article key={rule.id}>
            <div>
              <strong>{rule.rule_type.replace(/_/g, ' ')} · {rule.threshold}</strong>
              <small>
                {rule.destination.configured ? `Webhook: ${rule.destination.host}` : 'No delivery channel'}
                {rule.pause_reason ? ` · Paused: ${rule.pause_reason.replace(/_/g, ' ')}` : ''}
              </small>
              <small>
                {rule.last_delivery_success_at
                  ? `Last success ${new Date(rule.last_delivery_success_at).toLocaleString()}`
                  : rule.last_delivery_error_at
                    ? `Last error ${new Date(rule.last_delivery_error_at).toLocaleString()}`
                    : 'No delivery attempts yet'}
              </small>
            </div>
            <div className="owner-settings__rule-actions">
              <button
                type="button"
                disabled={!rule.destination.configured || busyRuleId === rule.id}
                onClick={() => void sendTest(rule.id)}
              >
                Send test
              </button>
              <button
                type="button"
                disabled={busyRuleId === rule.id}
                onClick={() => void removeRule(rule.id)}
              >
                Remove
              </button>
            </div>
          </article>
        ))}
      </div>
      <section className="owner-settings__history" aria-labelledby="owner-delivery-history">
        <h3 id="owner-delivery-history">Delivery history</h3>
        {deliveries.length === 0 ? <p>No alert deliveries yet.</p> : deliveries.map((delivery) => (
          <details key={delivery.id}>
            <summary>
              <span>{delivery.is_test ? 'Test' : delivery.rule_type.replace(/_/g, ' ')}</span>
              <span>{delivery.destination_host}</span>
              <span className={`owner-delivery-status owner-delivery-status--${delivery.status}`}>{delivery.status.replace(/_/g, ' ')}</span>
            </summary>
            <p>
              Node {delivery.node_id.slice(0, 12)}… · {delivery.attempts} attempt{delivery.attempts === 1 ? '' : 's'} ·
              {' '}{new Date(delivery.created_at).toLocaleString()}
            </p>
            {delivery.last_error && <p role="alert">{delivery.last_error}</p>}
            {delivery.attempt_history.length > 0 && (
              <ol>
                {delivery.attempt_history.map((attempt) => (
                  <li key={attempt.attempt}>
                    Attempt {attempt.attempt}: {attempt.outcome}
                    {attempt.httpStatus ? ` (HTTP ${attempt.httpStatus})` : ''}
                    {attempt.error ? ` — ${attempt.error}` : ''}
                  </li>
                ))}
              </ol>
            )}
          </details>
        ))}
      </section>
    </section>
  );
}
