import { useSyncExternalStore } from 'react';
import {
  activateServiceWorkerUpdate,
  deferServiceWorkerUpdate,
  getServiceWorkerUpdateSnapshot,
  subscribeServiceWorkerUpdates,
} from '../serviceWorkerUpdates.js';

export function ServiceWorkerUpdatePrompt() {
  const update = useSyncExternalStore(
    subscribeServiceWorkerUpdates,
    getServiceWorkerUpdateSnapshot,
    getServiceWorkerUpdateSnapshot,
  );
  if (!update.available) return null;
  return (
    <aside className="service-worker-update" role="status" aria-live="polite">
      <div>
        <strong>{update.blocked ? 'Finish the open task before updating' : 'Site update ready'}</strong>
        <span>
          {update.blocked
            ? 'The update is deferred while a dialog or unsaved owner form is open.'
            : 'Apply it now, or keep working and update later.'}
        </span>
      </div>
      <button type="button" onClick={() => activateServiceWorkerUpdate()} disabled={update.applying}>
        {update.applying ? 'Updating…' : 'Update now'}
      </button>
      <button type="button" onClick={deferServiceWorkerUpdate} disabled={update.applying}>
        Later
      </button>
    </aside>
  );
}
