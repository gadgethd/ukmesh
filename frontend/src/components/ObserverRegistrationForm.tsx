import { FormEvent, useState } from 'react';

export function ObserverRegistrationForm() {
  const [publicKey, setPublicKey] = useState('');
  const [iata, setIata] = useState('');
  const [name, setName] = useState('');
  const [contact, setContact] = useState('');
  const [message, setMessage] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    setSubmitting(true);
    setMessage(null);
    fetch('/api/observers/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ publicKey, iata, name, contact }),
    })
      .then(async (response) => {
        const value = await response.json().catch(() => ({})) as { error?: string; requestId?: string };
        if (!response.ok) throw new Error(value.error ?? `HTTP ${response.status}`);
        setMessage(`Request ${value.requestId ?? ''} submitted. An operator will provision the MQTT account.`);
      })
      .catch((error: Error) => setMessage(error.message))
      .finally(() => setSubmitting(false));
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
