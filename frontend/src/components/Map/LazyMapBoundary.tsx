import React, { Suspense } from 'react';
import { LoadingIndicator } from '../LoadingIndicator.js';

/** Keep the page and its reserved map space usable if the renderer chunk fails. */
export class LazyMapBoundary extends React.Component<
  React.PropsWithChildren<{ className: string; label: string; resource?: string; fallback?: React.ReactNode; onClose?: () => void }>,
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
    const { className, label, children, onClose, resource = 'map' } = this.props;
    const placeholder = (content: React.ReactNode) => (
      <div className={className} aria-label={label}>{content}{onClose && <button type="button" onClick={onClose}>Close</button>}</div>
    );
    if (this.state.failed) return placeholder(
      <div role="alert">
        <p>The {resource} could not be loaded.</p>
        <button type="button" onClick={() => window.location.reload()}>Reload {resource}</button>
      </div>,
    );
    const loading = this.props.fallback ?? placeholder(<LoadingIndicator label={`Loading ${resource}…`} />);
    if (!this.state.ready) return loading;
    return <Suspense fallback={loading}>{children}</Suspense>;
  }
}
