export type ApiScope = {
  network?: string;
  observer?: string;
};

export class ApiResponseError extends Error {
  readonly status: number;
  readonly url: string;

  constructor(message: string, status: number, url: string) {
    super(message);
    this.name = 'ApiResponseError';
    this.status = status;
    this.url = url;
  }
}

export type FetchJsonOptions<T> = {
  timeoutMs?: number;
  maxBytes?: number;
  validate?: (value: unknown) => value is T;
};

export function withScopeParams(path: string, scope: ApiScope = {}): string {
  const params = new URLSearchParams();
  if (scope.network) params.set('network', scope.network);
  if (scope.observer) params.set('observer', scope.observer);
  const query = params.toString();
  if (!query) return path;
  const sep = path.includes('?') ? '&' : '?';
  return `${path}${sep}${query}`;
}

export function statsEndpoint(scope: ApiScope = {}): string {
  return withScopeParams('/api/stats', scope);
}

export function chartStatsEndpoint(scope: ApiScope = {}): string {
  return withScopeParams('/api/stats/charts', scope);
}

export function uncachedEndpoint(url: string): string {
  const sep = url.includes('?') ? '&' : '?';
  return `${url}${sep}_ts=${Date.now()}`;
}

export async function checkedJson<T>(
  response: Response,
  options: Pick<FetchJsonOptions<T>, 'maxBytes' | 'validate'> = {},
): Promise<T> {
  const maxBytes = options.maxBytes ?? 5 * 1024 * 1024;
  if (!response.ok) {
    throw new ApiResponseError(
      `Request failed (${response.status})`,
      response.status,
      response.url,
    );
  }
  const declaredLength = Number(response.headers.get('content-length'));
  if (Number.isFinite(declaredLength) && declaredLength > maxBytes) {
    throw new ApiResponseError('Response exceeds byte limit', response.status, response.url);
  }
  const body = await response.text();
  if (new TextEncoder().encode(body).byteLength > maxBytes) {
    throw new ApiResponseError('Response exceeds byte limit', response.status, response.url);
  }
  let value: unknown;
  try {
    value = JSON.parse(body);
  } catch {
    throw new ApiResponseError('Response was not valid JSON', response.status, response.url);
  }
  if (options.validate && !options.validate(value)) {
    throw new ApiResponseError('Response did not match the expected schema', response.status, response.url);
  }
  return value as T;
}

export async function fetchJson<T>(
  input: RequestInfo | URL,
  init: RequestInit = {},
  options: FetchJsonOptions<T> = {},
): Promise<T> {
  const timeoutSignal = AbortSignal.timeout(options.timeoutMs ?? 15_000);
  const signal = init.signal
    ? AbortSignal.any([init.signal, timeoutSignal])
    : timeoutSignal;
  const headers = new Headers(init.headers);
  if (!headers.has('Accept')) headers.set('Accept', 'application/json');
  const response = await fetch(input, {
    ...init,
    signal,
    headers,
  });
  return checkedJson<T>(response, options);
}
