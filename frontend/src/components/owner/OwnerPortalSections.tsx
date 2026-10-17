import { type FormEvent } from 'react';
import { LoadingIndicator } from '../LoadingIndicator.js';
import type { OwnerPacketSharing, OwnerPublicFeed } from '../../pages/owner/ownerPortalModel.js';

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

export function OwnerPacketSharingSection(props: {
  settings: OwnerPacketSharing | null;
  loading: boolean;
  saving: boolean;
  error: string | null;
  selectedDestinationIds: string[];
  onDestination: (destinationId: string, selected: boolean) => void;
}) {
  if (props.loading) {
    return (
      <section className="prose-section owner-sharing-panel">
        <LoadingIndicator label="Loading packet sharing settings..." variant="block" />
      </section>
    );
  }
  if (!props.settings) {
    if (!props.error) return null;
    return (
      <section className="prose-section owner-sharing-panel" aria-labelledby="owner-sharing-heading">
        <h2 id="owner-sharing-heading">Share packets</h2>
        <p className="prose-note owner-sharing-error" role="alert">Unable to load packet-sharing settings: {props.error}</p>
      </section>
    );
  }
  if (!props.settings.featureEnabled) return null;

  const settings = props.settings;
  const availableDestinations = settings.destinations.filter((destination) => destination.configured);
  const selectedDestinations = availableDestinations.filter((destination) => props.selectedDestinationIds.includes(destination.id));
  return (
    <section className="prose-section owner-sharing-panel" aria-labelledby="owner-sharing-heading">
      <div className="owner-panel__head">
        <div className="owner-sharing-panel__intro">
          <p className="owner-sharing-panel__eyebrow">Destination controls</p>
          <h2 id="owner-sharing-heading">Share packets</h2>
          <p className="prose-note">Choose which approved sites receive validated packets. Each site has its own switch.</p>
        </div>
      </div>
      <div className={`owner-sharing-state${selectedDestinations.length > 0 ? ' owner-sharing-state--on' : ''}`} aria-live="polite">
        <span className="owner-sharing-state__dot" aria-hidden="true" />
        <div>
          <strong>{selectedDestinations.length > 0 ? `${selectedDestinations.length} site${selectedDestinations.length === 1 ? '' : 's'} enabled` : 'Packet sharing is off'}</strong>
          <p>
            {selectedDestinations.length > 0
              ? 'New eligible packets will be forwarded to the enabled sites.'
              : 'Turn on a site below whenever you want to start forwarding.'}
          </p>
        </div>
      </div>
      {settings.destinations.length < 1 ? (
        <p className="prose-note owner-sharing-empty">No approved destination sites are configured yet.</p>
      ) : (
        <>
          {availableDestinations.length < 1 ? (
            <p className="prose-note owner-sharing-empty">Approved sites are waiting for broker details.</p>
          ) : null}
          <div className="owner-sharing-destinations">
            {settings.destinations.map((destination) => (
              <div key={destination.id} className={`owner-sharing-destination${destination.configured ? '' : ' owner-sharing-destination--pending'}`}>
                <span className="owner-sharing-destination__body">
                  {destination.websiteUrl ? (
                    <a
                      className="owner-sharing-destination__link"
                      href={destination.websiteUrl}
                      target="_blank"
                      rel="noreferrer"
                      title={destination.description ?? undefined}
                    >
                      {destination.name}
                    </a>
                  ) : <strong>{destination.name}</strong>}
                  <small>{destination.configured
                    ? (destination.lastForwardedAt
                      ? 'Last packet forwarded successfully'
                      : (props.selectedDestinationIds.includes(destination.id) ? 'Waiting for first packet' : 'Ready to enable'))
                    : 'Waiting for broker details'}</small>
                </span>
                <label className="owner-sharing-switch" title={`Turn packet sharing ${props.selectedDestinationIds.includes(destination.id) ? 'off' : 'on'} for ${destination.name}`}>
                  <span className="ui-visually-hidden">Share packets with {destination.name}</span>
                  <input
                    type="checkbox"
                    role="switch"
                    checked={props.selectedDestinationIds.includes(destination.id)}
                    disabled={!destination.configured || props.saving}
                    onChange={(event) => props.onDestination(destination.id, event.target.checked)}
                  />
                  <span className="owner-sharing-switch__track" aria-hidden="true">
                    <span className="owner-sharing-switch__thumb" />
                  </span>
                </label>
              </div>
            ))}
          </div>
        </>
      )}
      {props.saving ? <p className="prose-note owner-sharing-saving">Saving…</p> : null}
      {props.error ? <p className="prose-note owner-sharing-error" role="alert">{props.error}</p> : null}
    </section>
  );
}

export function OwnerPublicFeedSection(props: {
  settings: OwnerPublicFeed | null;
  loading: boolean;
  saving: boolean;
  error: string | null;
  onEnabled: (enabled: boolean) => void;
}) {
  if (props.loading) {
    return (
      <section className="prose-section owner-sharing-panel owner-public-feed-panel">
        <LoadingIndicator label="Loading public-feed settings..." variant="block" />
      </section>
    );
  }
  if (!props.settings) {
    if (!props.error) return null;
    return (
      <section className="prose-section owner-sharing-panel owner-public-feed-panel" aria-labelledby="owner-public-feed-heading">
        <h2 id="owner-public-feed-heading">Public feed</h2>
        <p className="prose-note owner-sharing-error" role="alert">Unable to load public-feed settings: {props.error}</p>
      </section>
    );
  }
  if (!props.settings.featureEnabled) return null;

  const settings = props.settings;
  const active = settings.enabled && settings.configured && settings.eligible;
  const waitingForBroker = settings.enabled && !settings.configured;
  const waitingForActivity = settings.enabled && settings.configured && !settings.eligible;
  const lockedForActivity = !settings.enabled && !settings.eligible;
  const switchDisabled = props.saving
    || (!settings.enabled && (!settings.configured || !settings.eligible));
  const brokerEndpoint = settings.brokerUrl ?? 'the configured broker';
  return (
    <section className="prose-section owner-sharing-panel owner-public-feed-panel" aria-labelledby="owner-public-feed-heading">
      <div className="owner-panel__head">
        <div className="owner-sharing-panel__intro">
          <p className="owner-sharing-panel__eyebrow">Shared MQTT access</p>
          <h2 id="owner-public-feed-heading">Public feed</h2>
          <p className="prose-note">
            Opt into the authentication-locked UKmesh MQTT feed for read-only access to the curated MeshCore packet stream.
          </p>
        </div>
        <div className="owner-public-feed-switch-control">
          <span className="owner-public-feed-switch__label">{settings.enabled ? 'On' : 'Off'}</span>
          <label className="owner-sharing-switch owner-public-feed-switch" title={switchDisabled && lockedForActivity ? 'Publish an eligible packet in the last 24 hours to unlock the public feed' : `Turn the public feed ${settings.enabled ? 'off' : 'on'}`}>
            <span className="ui-visually-hidden">Publish packets and enable read-only public feed access</span>
            <input
              type="checkbox"
              role="switch"
              checked={settings.enabled}
              disabled={switchDisabled}
              onChange={(event) => props.onEnabled(event.target.checked)}
            />
            <span className="owner-sharing-switch__track" aria-hidden="true">
              <span className="owner-sharing-switch__thumb" />
            </span>
          </label>
        </div>
      </div>

      <div className="owner-public-feed-warning" role="note">
        <div className="owner-public-feed-warning__title"><span aria-hidden="true">!</span><strong>Internet-visible packet sharing</strong></div>
        <p>Packets will be observable over the internet.</p>
        <p>Broker: <code>{brokerEndpoint}</code>. When you opt in and have published an eligible packet in the last 24 hours, use the same MQTT username and password you use to send packets to subscribe to <code>public/+/+/packets</code>. Your public-feed access is read-only.</p>
        <p>The site operator controls MQTT access, but cannot verify what external users do with the data. Use your own discretion.</p>
      </div>

      <div className={`owner-sharing-state${active ? ' owner-sharing-state--on' : ''}`} aria-live="polite">
        <span className="owner-sharing-state__dot" aria-hidden="true" />
        <div>
          <strong>{active
            ? 'Public feed is on'
            : waitingForBroker
              ? 'Public feed is waiting for the broker'
              : waitingForActivity
                ? 'Public feed access is paused'
                : lockedForActivity
                  ? 'Public feed is locked until you are active'
                  : 'Public feed is off'}</strong>
          <p>
            {active
            ? `Your existing MQTT login has read-only access to the curated opted-in packet feed on ${brokerEndpoint}.`
              : waitingForBroker
                ? 'It remains enabled for your account, but access is paused until the operator restores the broker configuration.'
                : waitingForActivity
                ? `No eligible packet has been observed from your nodes in the last 24 hours. Access will return after activity resumes.`
                : lockedForActivity
                    ? 'Publish at least one eligible packet in the last 24 hours to unlock the opt-in switch.'
              : settings.configured
                ? 'Turn it on whenever you want read-only access to the opted-in packet topics.'
                : 'The operator has not configured the public broker yet.'}
          </p>
        </div>
      </div>

      <div className="owner-public-feed-details" aria-label="Public feed details">
        <div className="owner-public-feed-details__item"><span className="owner-public-feed-details__label">Broker</span><code>{brokerEndpoint}</code></div>
        <div className="owner-public-feed-details__item owner-public-feed-details__item--topics"><span className="owner-public-feed-details__label">Packet topic filter</span><code>{settings.topicFilters.length > 0 ? settings.topicFilters.join(', ') : 'Unavailable until configured'}</code></div>
        <div className="owner-public-feed-details__item"><span className="owner-public-feed-details__label">Recent eligible packets</span><span>{settings.recentPacketCount}</span></div>
        <div className="owner-public-feed-details__item"><span className="owner-public-feed-details__label">Access</span><span>Read-only for opted-in active owners and approved subscribers</span></div>
      </div>
      {props.saving ? <p className="prose-note owner-sharing-saving">Saving…</p> : null}
      {props.error ? <p className="prose-note owner-sharing-error" role="alert">{props.error}</p> : null}
    </section>
  );
}
