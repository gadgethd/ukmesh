import { useLayoutEffect, useState } from 'react';
import { nodeStore } from '../features/realtime/nodeStore.js';
import { linkStateStore } from '../features/realtime/linkStore.js';
import { scopeIsReady, scopeKey, type PublicScope } from '../utils/scope.js';

/** Store publication happens after commit; abandoned React renders cannot reset a session. */
export function useRealtimeScopeSession(scope: PublicScope, includeLinks = false) {
  const key = scopeKey(scope);
  const [session, setSession] = useState({ key: '', nodeEpoch: -1, linkEpoch: -1 });
  useLayoutEffect(() => {
    setSession({
      key,
      nodeEpoch: nodeStore.reset(key),
      linkEpoch: includeLinks ? linkStateStore.reset(key) : -1,
    });
  }, [key, includeLinks]);
  return { ...session, ready: session.key === key && scopeIsReady(scope) };
}
