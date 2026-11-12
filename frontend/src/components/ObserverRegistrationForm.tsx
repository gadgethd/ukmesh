import { FormEvent, useEffect, useRef, useState } from 'react';
import { fetchJson } from '../utils/api.js';

export function ObserverRegistrationForm() {
  const [publicKey, setPublicKey] = useState('');
  const [iata, setIata] = useState('');
  const [name, setName] = useState('');
  const [contact, setContact] = useState('');
  const [message, setMessage] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => request.current?.abort(), []);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    setSubmitting(true);
    setMessage(null);
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    fetchJson('/api/observers/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ publicKey, iata, name, contact }),
      signal: controller.signal,
    }, { maxBytes: 64 * 1024, readErrorMessage: true,
      validate: (value): value is { requestId: string } => !!value && typeof value === 'object'
        && 'requestId' in value && typeof value.requestId === 'string',
    })
      .then((value) => {
        if (controller.signal.aborted) return;
        setMessage(`Request ${value.requestId ?? ''} submitted. An operator will provision the MQTT account.`);
      })
      .catch((error: Error) => { if (!controller.signal.aborted) setMessage(error.message); })
      .finally(() => { if (!controller.signal.aborted) setSubmitting(false); });
  };

  return (
    <form className="observer-registration" onSubmit={submit}>
      <label>Observer public key<input value={publicKey} onChange={(event) => setPublicKey(event.target.value)} maxLength={64} required /></label>
      <label>IATA region<input value={iata} onChange={(event) => setIata(event.target.value.toUpperCase())} maxLength={8} required /></label>
      <label>Display name<input value={name} onChange={(event) => setName(event.target.value)} maxLength={100} /></label>
      <label>Discord/email contact<input value={contact} onChange={(event) => setContact(event.target.value)} maxLength={200} required /></label>
      <button type="submit" className="site-btn site-btn--primary" disabled={submitting}>
        {submitting ? 'Submitting…' : 'Request MQTT observer user'}
      </button>
      {message && <p className="prose-note" role="status">{message}</p>}
    </form>
  );
}
