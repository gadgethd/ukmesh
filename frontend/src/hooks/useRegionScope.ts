import { useCallback, useEffect, useState } from 'react';
import { hashWithRegion, regionFromHash } from '../utils/regionUrlState.js';

export function useRegionScope(enabled: boolean) {
  const [requested, setRequested] = useState(() => regionFromHash(window.location.hash));
  useEffect(() => {
    const restore = () => setRequested(regionFromHash(window.location.hash));
    window.addEventListener('hashchange', restore);
    window.addEventListener('popstate', restore);
    return () => {
      window.removeEventListener('hashchange', restore);
      window.removeEventListener('popstate', restore);
    };
  }, []);
  const select = useCallback((region?: string) => {
    const url = new URL(window.location.href);
    url.hash = hashWithRegion(url.hash, region);
    if (url.href !== window.location.href) window.history.pushState(null, '', url);
    setRequested(regionFromHash(url.hash));
  }, []);
  return { region: enabled ? requested : undefined, select };
}
