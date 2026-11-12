import React, { Suspense } from 'react';
import { LoadingIndicator } from '../LoadingIndicator.js';

/** Keep the page and its reserved map space usable if the renderer chunk fails. */
export class LazyMapBoundary extends React.Component<
  React.PropsWithChildren<{ className: string; label: string; onClose?: () => void }>,
  { failed: boolean; ready: boolean }
> {
  state = { failed: false, ready: false };
  private frame: number | null = null;
  static getDerivedStateFromError() { return { failed: true }; }

  componentDidMount() {
    // Give the heading, controls and reserved workspace a paint before module
    // evaluation/WebGL startup. No elapsed-time delay or idle timeout: advance
    // at the next painted frame, and naturally wait while the page is hidden.
    this.frame = requestAnimationFrame(() => {
      this.frame = requestAnimationFrame(() => { this.frame = null; this.setState({ ready: true }); });
    });
  }

  componentWillUnmount() {
    if (this.frame !== null) cancelAnimationFrame(this.frame);
  }

  render() {
    const { className, label, children, onClose } = this.props;
    const placeholder = (content: React.ReactNode) => (
      <div className={className} aria-label={label}>{content}{onClose && <button type="button" onClick={onClose}>Close</button>}</div>
    );
    if (this.state.failed) return placeholder(
      <div role="alert">
        <p>The map could not be loaded.</p>
        <button type="button" onClick={() => window.location.reload()}>Reload map</button>
      </div>,
    );
    const loading = placeholder(<LoadingIndicator label="Loading map…" />);
    if (!this.state.ready) return loading;
    return <Suspense fallback={loading}>{children}</Suspense>;
  }
}
