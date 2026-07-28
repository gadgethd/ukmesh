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

type Rule = { id: string; node_id: string; rule_type: string; threshold: number; channels: { webhook?: string }; enabled: boolean };
async function ownerCsrf(): Promise<string> {
  const response = await fetch('/api/owner/csrf', { cache: 'no-store' });
  const value = await response.json() as { csrfToken?: string };
  if (!response.ok || !value.csrfToken) throw new Error('Could not prepare secure request');
  return value.csrfToken;
}

export function OwnerAlertSettings({ nodes, selectedNodeId }: { nodes: Array<{ node_id: string; name?: string | null }>; selectedNodeId: string }) {
  const [rules, setRules] = useState<Rule[]>([]);
  const [ruleType, setRuleType] = useState('offline_minutes');
  const [threshold, setThreshold] = useState('30');
  const [webhook, setWebhook] = useState('');
  const [message, setMessage] = useState<string | null>(null);
  const load = () => fetch('/api/owner/alert-rules').then((response) => response.ok ? response.json() as Promise<Rule[]> : []).then(setRules).catch(() => {});
  useEffect(() => { void load(); }, []);
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
  return (
    <section className="prose-section owner-settings">
      <h2>Alert settings</h2>
      <p>Create per-node health rules. Webhooks are delivered at most once per hour while a condition remains active.</p>
      <form className="owner-settings__form" onSubmit={submit}>
        <label>Node<select value={selectedNodeId} disabled>{nodes.map((node) => <option key={node.node_id} value={node.node_id}>{node.name ?? node.node_id}</option>)}</select></label>
        <label>Condition<select value={ruleType} onChange={(event) => setRuleType(event.target.value)}><option value="offline_minutes">Offline minutes</option><option value="battery_below_mv">Battery below mV</option><option value="link_loss_above_db">Path loss above dB</option></select></label>
        <label>Threshold<input type="number" min="1" value={threshold} onChange={(event) => setThreshold(event.target.value)} /></label>
        <label>HTTPS webhook (optional)<input type="url" value={webhook} onChange={(event) => setWebhook(event.target.value)} /></label>
        <button type="submit" className="site-btn site-btn--primary">Save rule</button>
      </form>
      {message && <p role="status">{message}</p>}
      <div className="owner-settings__rules">{rules.map((rule) => <article key={rule.id}><span>{rule.rule_type.replace(/_/g, ' ')} · {rule.threshold}</span><button type="button" onClick={() => void ownerCsrf().then((csrfToken) => fetch(`/api/owner/alert-rules/${rule.id}`, { method: 'DELETE', headers: { 'X-CSRF-Token': csrfToken } })).then(load)}>Remove</button></article>)}</div>
    </section>
  );
}
