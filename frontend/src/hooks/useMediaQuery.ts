import { useCallback, useMemo, useSyncExternalStore } from 'react';

export function useMediaQuery(query: string) {
  const media = useMemo(() => typeof window === 'undefined' ? null : window.matchMedia(query), [query]);
  const subscribe = useCallback((listener: () => void) => {
    media?.addEventListener('change', listener);
    return () => media?.removeEventListener('change', listener);
  }, [media]);
  return useSyncExternalStore(subscribe, () => media?.matches ?? false, () => false);
}
