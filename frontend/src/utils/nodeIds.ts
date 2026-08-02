export function canonicalNodeId(value: string): string {
  return value.trim().toUpperCase();
}

export function canonicalOptionalNodeId(value: string | null | undefined): string | undefined {
  if (typeof value !== 'string') return undefined;
  const normalized = canonicalNodeId(value);
  return normalized || undefined;
}
