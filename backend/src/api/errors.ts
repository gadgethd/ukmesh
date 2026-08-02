import { randomUUID } from 'node:crypto';
import type {
  ErrorRequestHandler,
  NextFunction,
  Request,
  RequestHandler,
  Response,
  Router,
} from 'express';

export class ApiInputError extends Error {
  constructor(
    message: string,
    readonly code = 'INVALID_INPUT',
    readonly status = 400,
  ) {
    super(message);
  }
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
    res.status(error.status).json({
      error: error.message,
      code: error.code,
      requestId,
    });
    return;
  }
  const bodyError = error as {
    type?: string;
    status?: number;
    message?: string;
  };
  if (bodyError.type === 'entity.too.large') {
    res.status(413).json({
      error: 'Request body is too large',
      code: 'BODY_TOO_LARGE',
      requestId,
    });
    return;
  }
  if (bodyError.type === 'entity.parse.failed' || bodyError.status === 400) {
    res.status(400).json({
      error: 'Malformed request body',
      code: 'MALFORMED_BODY',
      requestId,
    });
    return;
  }
  console.error('[api] request failed', {
    requestId,
    method: req.method,
    path: req.originalUrl.split('?')[0],
    error: error instanceof Error ? error.message : String(error),
  });
  res.status(500).json({
    error: 'Internal server error',
    code: 'INTERNAL_ERROR',
    requestId,
  });
};
