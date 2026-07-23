import React from 'react';

type State = { error: Error | null };

export class AppErrorBoundary extends React.Component<React.PropsWithChildren, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo): void {
    void fetch('/api/telemetry/frontend-error', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        kind: 'crash',
        message: error.message,
        stack: `${error.stack ?? ''}\n${info.componentStack}`.slice(0, 4000),
        page: window.location.href,
        userAgent: navigator.userAgent,
      }),
      keepalive: true,
    }).catch(() => {});
  }

  render(): React.ReactNode {
    if (!this.state.error) return this.props.children;
    return (
      <main className="app-error" role="alert">
        <span className="app-error__mark" aria-hidden="true">◈</span>
        <h1>This view could not be loaded</h1>
        <p>The problem has been recorded. Reload the page to reconnect and request fresh network data.</p>
        <button type="button" onClick={() => window.location.reload()}>Reload page</button>
      </main>
    );
  }
}
