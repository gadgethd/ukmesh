export function createCollectorSlot<T>(fallback: T) {
  let current = fallback;
  return {
    read: () => current,
    register(value: T): () => void {
      current = value;
      return () => { if (current === value) current = fallback; };
    },
  };
}
