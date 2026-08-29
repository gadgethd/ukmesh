import { randomUUID } from 'node:crypto';
import type {
  ErrorRequestHandler,
  NextFunction,
  Request,
  RequestHandler,
  Response,
  Router,
} from 'express';

export const API_ERROR_CODES = Object.freeze({
  invalidInput: 'INVALID_INPUT',
  ambiguousParameter: 'AMBIGUOUS_PARAMETER',
  missingParameter: 'MISSING_PARAMETER',
  invalidInteger: 'INVALID_INTEGER',
  integerOutOfRange: 'INTEGER_OUT_OF_RANGE',
  invalidString: 'INVALID_STRING',
  invalidHexIdentifier: 'INVALID_HEX_IDENTIFIER',
  invalidCoordinate: 'INVALID_COORDINATE',
  coordinateOutOfRange: 'COORDINATE_OUT_OF_RANGE',
  invalidFloat: 'INVALID_FLOAT',
  floatOutOfRange: 'FLOAT_OUT_OF_RANGE',
  invalidBoolean: 'INVALID_BOOLEAN',
  invalidEnum: 'INVALID_ENUM',
  invalidCursor: 'INVALID_CURSOR',
  invalidObserver: 'INVALID_OBSERVER',
  invalidNetworkScope: 'INVALID_NETWORK_SCOPE',
  invalidFields: 'INVALID_FIELDS',
  invalidSnapshot: 'INVALID_SNAPSHOT',
  invalidCredentials: 'INVALID_CREDENTIALS',
  invalidAlertRule: 'INVALID_ALERT_RULE',
  invalidWebhookDestination: 'INVALID_WEBHOOK_DESTINATION',
  invalidLinkId: 'INVALID_LINK_ID',
  invalidObserverRegistration: 'INVALID_OBSERVER_REGISTRATION',
  invalidIncidentId: 'INVALID_INCIDENT_ID',
  invalidNodeId: 'INVALID_NODE_ID',
  missingMessage: 'MISSING_MESSAGE',
  invalidTransport: 'INVALID_TRANSPORT',
  invalidIdempotencyKey: 'INVALID_IDEMPOTENCY_KEY',
  bodyTooLarge: 'BODY_TOO_LARGE',
  malformedBody: 'MALFORMED_BODY',
  internalError: 'INTERNAL_ERROR',
} as const);

export type ApiErrorCode = typeof API_ERROR_CODES[keyof typeof API_ERROR_CODES];

export class ApiInputError extends Error {
  constructor(
    message: string,
    readonly code: ApiErrorCode = API_ERROR_CODES.invalidInput,
    readonly status = 400,
  ) {
    super(message);
    this.name = 'ApiInputError';
  }
}

export function sendApiError(
  res: Response,
  status: number,
  message: string,
  code: ApiErrorCode,
): void {
  const requestId = String(res.locals['requestId'] ?? randomUUID());
  if (!res.hasHeader('X-Request-Id')) res.setHeader('X-Request-Id', requestId);
  res.status(status).json({ error: message, code, requestId });
}

type ExpressLayer = {
  handle?: RequestHandler & { stack?: ExpressLayer[] };
  route?: { stack?: ExpressLayer[] };
};

function wrapLayer(layer: ExpressLayer): void {
  for (const child of layer.route?.stack ?? layer.handle?.stack ?? []) wrapLayer(child);
  const original = layer.handle;
  if (!original || original.length === 4 || original.stack) return;
  layer.handle = function promiseAwareHandler(req, res, next) {
    try {
      Promise.resolve(original(req, res, next)).catch(next);
    } catch (error) {
      next(error);
    }
  };
}

/** Bridge rejected async handlers into Express 4's error pipeline. */
export function wrapAsyncHandlers(router: Router): void {
  const stack = (router as unknown as { stack?: ExpressLayer[] }).stack ?? [];
  for (const layer of stack) wrapLayer(layer);
}

export const requestContextMiddleware: RequestHandler = (_req, res, next) => {
  const requestId = randomUUID();
  res.locals['requestId'] = requestId;
  res.setHeader('X-Request-Id', requestId);
  next();
};

export const apiErrorMiddleware: ErrorRequestHandler = (
  error: unknown,
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  if (res.headersSent) {
    next(error);
    return;
  }
  const requestId = String(res.locals['requestId'] ?? randomUUID());
  if (error instanceof ApiInputError) {
    sendApiError(res, error.status, error.message, error.code);
    return;
  }
  const bodyError = error as {
    type?: string;
    status?: number;
    message?: string;
  };
  if (bodyError.type === 'entity.too.large') {
    sendApiError(res, 413, 'Request body is too large', API_ERROR_CODES.bodyTooLarge);
    return;
  }
  if (bodyError.type === 'entity.parse.failed' || bodyError.status === 400) {
    sendApiError(res, 400, 'Malformed request body', API_ERROR_CODES.malformedBody);
    return;
  }
  console.error('[api] request failed', {
    requestId,
    method: req.method,
    path: req.originalUrl.split('?')[0],
    error: error instanceof Error ? error.message : String(error),
  });
  sendApiError(res, 500, 'Internal server error', API_ERROR_CODES.internalError);
};
