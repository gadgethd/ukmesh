export function normalizeObserverQuery(value: unknown): string | undefined {
  const observer = String(value ?? '').trim().toUpperCase();
  return observer && /^[0-9A-F]{64}$/.test(observer) ? observer : undefined;
}
