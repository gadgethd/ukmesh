/**
 * Legacy entry point retained so an older deployment cannot keep mutating ACLs
 * after a rolling update. Reconciliation now runs in the backend process from
 * verified operator grants only.
 */
console.warn('[acl-watcher] disabled: broker logs are audit-only and cannot modify authorization');
