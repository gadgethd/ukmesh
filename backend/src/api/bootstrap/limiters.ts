import { rateLimit } from 'express-rate-limit';

export function parseApiRateLimitMax(
  value = process.env['API_RATE_LIMIT_MAX'],
): number {
  const parsed = Number(value ?? 120);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 1_000_000) {
    throw new Error('API_RATE_LIMIT_MAX must be an integer from 1 to 1000000');
  }
  return parsed;
}

export function createGlobalApiLimiter() {
  return rateLimit({
    windowMs: 60_000,
    max: parseApiRateLimitMax(),
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests, please slow down' },
  });
}

export const OWNER_LOGIN_LIMITER = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 8,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many login attempts, try again in 15 minutes' },
});

export const PATH_BETA_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many path requests, slow down' },
});

export const COVERAGE_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 12,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many coverage requests, slow down' },
});

export const PATH_LEARNING_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 30,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many path learning requests, slow down' },
});

export const EXPENSIVE_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 12,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests, slow down' },
});

export const STATS_CHARTS_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 12,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many stats chart requests, slow down' },
});

export const PACKET_DETAIL_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 30,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many packet detail requests, slow down' },
});

export const NODES_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many node requests, slow down' },
});

export const TELEMETRY_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many diagnostic reports' },
});

// Exports have an independent budget so downloads do not consume the shared
// expensive-query allowance.
export const EXPORT_LIMITER = rateLimit({
  windowMs: 60_000,
  max: 5,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many export requests, slow down' },
});
