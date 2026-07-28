import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import type { NextFunction, Request, Response } from 'express';

function digest(value: string): Buffer {
  return createHash('sha256').update(value, 'utf8').digest();
}

export function operatorTokenIsConfigured(expected: string | undefined): expected is string {
  return typeof expected === 'string' && expected.length >= 32;
}

export function verifyOperatorToken(expected: string | undefined, provided: string | undefined): boolean {
  if (!operatorTokenIsConfigured(expected) || typeof provided !== 'string') return false;
  return timingSafeEqual(digest(expected), digest(provided));
}

export function createCsrfToken(): string {
  return randomBytes(32).toString('base64url');
}

export function readCookie(cookieHeader: string | undefined, name: string): string | null {
  for (const part of String(cookieHeader ?? '').split(';')) {
    const [rawName, ...rawValue] = part.trim().split('=');
    if (rawName !== name) continue;
    try {
      return decodeURIComponent(rawValue.join('='));
    } catch {
      return null;
    }
  }
  return null;
}

export function verifyDoubleSubmitCsrf(req: Request, cookieName: string): boolean {
  const cookieToken = readCookie(req.headers.cookie, cookieName);
  const headerToken = String(req.headers['x-csrf-token'] ?? '');
  if (!cookieToken || !headerToken) return false;
  return timingSafeEqual(digest(cookieToken), digest(headerToken));
}

export function requireDoubleSubmitCsrf(cookieName: string) {
  return (req: Request, res: Response, next: NextFunction): void => {
    if (!verifyDoubleSubmitCsrf(req, cookieName)) {
      res.status(403).json({ error: 'Invalid CSRF token' });
      return;
    }
    next();
  };
}

const MAP_GEOLOCATION_PATHS = new Set(['/', '/feed', '/repeater']);

export function applySecurityHeaders(req: Pick<Request, 'path'>, res: Response): void {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  const geolocation = MAP_GEOLOCATION_PATHS.has(req.path) ? '(self)' : '()';
  res.setHeader('Permissions-Policy', `geolocation=${geolocation}, camera=(), microphone=()`);
}
