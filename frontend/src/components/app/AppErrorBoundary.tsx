import React from 'react';
import { postTelemetry } from '../../telemetry/clientErrors.js';

type State = { error: Error | null };

export class AppErrorBoundary extends React.Component<React.PropsWithChildren, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: React.ErrorInfo): void {
    postTelemetry({
      kind: 'crash',
      message: error.message,
      stack: `${error.stack ?? ''}\n${info.componentStack}`,
    });
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
