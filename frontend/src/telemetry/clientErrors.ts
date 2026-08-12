/**
 * Client-side error capture for BOTH domains (app.ukmesh.com + ukmesh.com).
 * Installed once from main.tsx before render. GDPR-minimal: sends kind,
 * truncated message/stack, page PATHNAME only (no query strings), userAgent.
 * No raw IP, no user identifiers, no cookies, no localStorage.
 */

const TELEMETRY_OFF = new URLSearchParams(window.location.search).has('telemetry=off');

const MAX_MESSAGE = 500;
const MAX_STACK = 4000;
const MAX_RESOURCE_MESSAGE = 200;

export type ClassifiedError = {
  kind: 'error' | 'warning' | 'unhandledrejection';
  message: string;
  stack?: string;
};

/** Known-benign noise that should never be reported. */
const NOISE_PATTERNS: readonly RegExp[] = [
  /\/terrain-tiles\//,
];

export function isTelemetryDisabled(): boolean {
  return TELEMETRY_OFF;
}

export function isNoise(message: string): boolean {
  return NOISE_PATTERNS.some((pattern) => pattern.test(message));
}

function originStripped(value: string): string {
  return value.replace(/^https?:\/\/[^/]+/i, '');
}

/**
 * Pure classifier, duck-typed so it is unit-testable without DOM globals.
 * Returns null for unactionable noise (empty message, no element target).
 */
export function classifyErrorEvent(event: unknown): ClassifiedError | null {
  if (event && typeof event === 'object' && 'reason' in event) {
    const reason = (event as { reason: unknown }).reason;
    const error = reason instanceof Error ? reason : new Error(String(reason));
    return {
      kind: 'unhandledrejection',
      message: (error.message || 'Unhandled promise rejection').slice(0, MAX_MESSAGE),
      stack: error.stack,
    };
  }
  const errorEvent = event as { message?: unknown; target?: unknown; error?: unknown } | null;
  const message = typeof errorEvent?.message === 'string' ? errorEvent.message : '';
  if (message) {
    const stack = errorEvent?.error instanceof Error ? errorEvent.error.stack : undefined;
    return { kind: 'error', message: message.slice(0, MAX_MESSAGE), stack };
  }
  const target = errorEvent?.target;
  if (target && typeof target === 'object' && target !== null && 'tagName' in target) {
    const tag = String((target as { tagName: unknown }).tagName).toLowerCase();
    const element = target as { src?: unknown; href?: unknown };
    const resource =
      typeof element.src === 'string' ? element.src : typeof element.href === 'string' ? element.href : '';
    return {
      kind: 'error',
      message: `Resource failed: ${tag} ${originStripped(resource)}`.slice(0, MAX_RESOURCE_MESSAGE),
    };
  }
  // Empty message with no element target — unactionable noise.
  return null;
}

export function postTelemetry(classified: ClassifiedError): void {
  if (TELEMETRY_OFF) return;
  void fetch('/api/telemetry/frontend-error', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      kind: classified.kind,
      message: classified.message,
      stack: classified.stack?.slice(0, MAX_STACK),
      page: window.location.pathname,
      userAgent: navigator.userAgent,
    }),
    keepalive: true,
  }).catch(() => {});
}

export function installClientErrorReporting(): void {
  if (TELEMETRY_OFF) return;

  window.addEventListener('error', (event) => {
    const classified = classifyErrorEvent(event);
    if (!classified || isNoise(classified.message)) return;
    postTelemetry(classified);
  });

  window.addEventListener('unhandledrejection', (event) => {
    const classified = classifyErrorEvent(event);
    if (!classified || isNoise(classified.message)) return;
    postTelemetry(classified);
  });

  const originalError = console.error;
  console.error = (...args: unknown[]) => {
    try {
      const message = args
        .map((arg) => (arg instanceof Error ? arg.message : String(arg)))
        .join(' ')
        .slice(0, MAX_MESSAGE);
      if (message && !isNoise(message)) {
        const stack = args.find((arg): arg is Error => arg instanceof Error)?.stack;
        postTelemetry({ kind: 'warning', message, stack });
      }
    } catch {
      // Never let telemetry break console.error itself.
    }
    originalError(...args);
  };
}
