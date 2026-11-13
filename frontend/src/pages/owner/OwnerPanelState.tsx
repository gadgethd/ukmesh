import React from 'react';
import { LoadingIndicator } from '../../components/LoadingIndicator.js';

export type OwnerPanelStateProps = {
  label: string;
  hasData: boolean;
  error: string | null;
  onRetry: () => void;
  children: React.ReactNode;
};

export function OwnerPanelState({ label, hasData, error, onRetry, children }: OwnerPanelStateProps) {
  // Keep the last successful snapshot mounted during background polls. Adding
  // a spinner above it changes the panel height and makes the whole dashboard
  // jump on every refresh.
  return <>
    {error ? (
      <div role="alert" className="prose-note owner-login__error">
        <p>{label} is temporarily unavailable.{hasData ? ' Showing the last successful data; it may be out of date.' : ' Please try again shortly.'}</p>
        <button type="button" className="site-btn site-btn--ghost" onClick={onRetry}>Retry {label}</button>
      </div>
    ) : !hasData ? (
      <LoadingIndicator label={`Loading ${label}…`} variant="inline" />
    ) : null}
    {hasData ? children : null}
  </>;
}

/** Shared panel spacing keeps loading, cached data and retry actions aligned. */
export function OwnerPanel({ className, title, description, ...state }: OwnerPanelStateProps & {
  className: string; title?: React.ReactNode; description?: string;
}) {
  return <section className={className}>
    <div className="owner-panel__head"><div>
      <h2>{title ?? state.label}</h2>
      {description && <p className="prose-note" style={{ marginTop: 0 }}>{description}</p>}
    </div></div>
    <OwnerPanelState {...state} />
  </section>;
}
