/**
 * Redis connection settings shared by API processes and workers.
 *
 * Keeping the password outside the URL means a normal `REDIS_URL` remains safe
 * when the password contains URI-reserved characters, and allows Compose to
 * authenticate every consumer without duplicating credentials in URLs.
 */
export function getRedisUrl(): string {
  return process.env['REDIS_URL'] ?? 'redis://redis:6379';
}

export function getRedisConnectionOptions(): { password?: string } {
  const password = process.env['REDIS_PASSWORD'];
  return password ? { password } : {};
}
