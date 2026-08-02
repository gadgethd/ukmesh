export class UpstreamRequestError extends Error {
  constructor(
    readonly code:
      | 'CIRCUIT_OPEN'
      | 'CONNECT_TIMEOUT'
      | 'TOTAL_TIMEOUT'
      | 'BAD_STATUS'
      | 'INVALID_CONTENT_TYPE'
      | 'RESPONSE_TOO_LARGE'
      | 'INVALID_JSON'
      | 'NETWORK_ERROR',
    message: string = code,
  ) {
    super(message);
  }
}

export class UpstreamCircuit {
  private failures = 0;
  private openedAt: number | null = null;
  private probeInFlight = false;

  constructor(
    private readonly failureThreshold: number,
    private readonly cooldownMs: number,
    private readonly now: () => number = Date.now,
  ) {}

  beforeRequest(): void {
    if (this.openedAt === null) return;
    if (this.now() - this.openedAt < this.cooldownMs || this.probeInFlight) {
      throw new UpstreamRequestError('CIRCUIT_OPEN');
    }
    this.probeInFlight = true;
  }

  success(): void {
    this.failures = 0;
    this.openedAt = null;
    this.probeInFlight = false;
  }

  failure(): void {
    this.probeInFlight = false;
    this.failures += 1;
    if (this.failures >= this.failureThreshold) this.openedAt = this.now();
  }

  snapshot(): { failures: number; open: boolean; probeInFlight: boolean } {
    return {
      failures: this.failures,
      open: this.openedAt !== null,
      probeInFlight: this.probeInFlight,
    };
  }
}

export async function fetchBoundedJson<T>(
  url: URL,
  init: RequestInit,
  options: {
    connectTimeoutMs: number;
    totalTimeoutMs: number;
    maxResponseBytes: number;
    circuit: UpstreamCircuit;
    fetchFn?: typeof fetch;
  },
): Promise<T> {
  options.circuit.beforeRequest();
  const connectController = new AbortController();
  const totalController = new AbortController();
  const connectTimer = setTimeout(() => {
    connectController.abort(new UpstreamRequestError('CONNECT_TIMEOUT'));
  }, options.connectTimeoutMs);
  const totalTimer = setTimeout(() => {
    totalController.abort(new UpstreamRequestError('TOTAL_TIMEOUT'));
  }, options.totalTimeoutMs);
  const signal = AbortSignal.any([
    connectController.signal,
    totalController.signal,
    ...(init.signal ? [init.signal] : []),
  ]);
  try {
    let response: Response;
    try {
      response = await (options.fetchFn ?? fetch)(url, {
        ...init,
        redirect: 'error',
        signal,
      });
    } catch (error) {
      if (signal.reason instanceof UpstreamRequestError) throw signal.reason;
      throw new UpstreamRequestError(
        'NETWORK_ERROR',
        error instanceof Error ? error.message : String(error),
      );
    } finally {
      clearTimeout(connectTimer);
    }
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined);
      throw new UpstreamRequestError('BAD_STATUS', `upstream returned ${response.status}`);
    }
    const contentType = response.headers.get('content-type')?.split(';')[0]?.trim().toLowerCase();
    if (contentType !== 'application/json') {
      await response.body?.cancel().catch(() => undefined);
      throw new UpstreamRequestError('INVALID_CONTENT_TYPE');
    }
    const contentLength = Number(response.headers.get('content-length'));
    if (Number.isFinite(contentLength) && contentLength > options.maxResponseBytes) {
      await response.body?.cancel().catch(() => undefined);
      throw new UpstreamRequestError('RESPONSE_TOO_LARGE');
    }
    if (!response.body) throw new UpstreamRequestError('INVALID_JSON');
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      bytes += next.value.byteLength;
      if (bytes > options.maxResponseBytes) {
        await reader.cancel().catch(() => undefined);
        throw new UpstreamRequestError('RESPONSE_TOO_LARGE');
      }
      chunks.push(next.value);
    }
    const body = new Uint8Array(bytes);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.byteLength;
    }
    let parsed: T;
    try {
      parsed = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(body)) as T;
    } catch {
      throw new UpstreamRequestError('INVALID_JSON');
    }
    options.circuit.success();
    return parsed;
  } catch (error) {
    options.circuit.failure();
    throw signal.reason instanceof UpstreamRequestError ? signal.reason : error;
  } finally {
    clearTimeout(connectTimer);
    clearTimeout(totalTimer);
  }
}
