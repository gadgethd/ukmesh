import React, { Suspense } from 'react';
import { LoadingIndicator } from '../LoadingIndicator.js';

/** Keep the page and its reserved map space usable if the renderer chunk fails. */
export class LazyMapBoundary extends React.Component<
  React.PropsWithChildren<{ className: string; label: string }>,
  { failed: boolean }
> {
  state = { failed: false };
  static getDerivedStateFromError() { return { failed: true }; }

  render() {
    const { className, label, children } = this.props;
    const placeholder = (content: React.ReactNode) => (
      <div className={className} aria-label={label}>{content}</div>
    );
    if (this.state.failed) return placeholder(
      <div role="alert">
        <p>The map could not be loaded.</p>
        <button type="button" onClick={() => window.location.reload()}>Reload map</button>
      </div>,
    );
    return <Suspense fallback={placeholder(<LoadingIndicator label="Loading map…" />)}>{children}</Suspense>;
  }
}
