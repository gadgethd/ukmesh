import { type FormEvent } from 'react';
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
