import { useSyncExternalStore } from 'react';
import { getCurrentSite } from '../config/site.js';

export type WatchCategory = 'node' | 'region' | 'observer' | 'packet_type' | 'spam_incident' | 'search';
export type WatchEntry = {
  category: WatchCategory;
  id: string;
  label: string;
  scope: string;
  createdAt: string;
};

const STORAGE_KEY = 'meshcore-watchlist-v1';
let entries: WatchEntry[] = readEntries();
const listeners = new Set<() => void>();

function readEntries(): WatchEntry[] {
  try {
    const parsed = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '[]') as unknown;
    return Array.isArray(parsed) ? parsed.filter((entry): entry is WatchEntry => Boolean(
      entry && typeof entry === 'object' && typeof (entry as WatchEntry).category === 'string'
      && typeof (entry as WatchEntry).id === 'string' && typeof (entry as WatchEntry).label === 'string',
    )).map((entry) => ({
      ...entry,
      scope: typeof entry.scope === 'string' && entry.scope ? entry.scope : currentWatchScope(),
    })).slice(0, 100) : [];
  } catch {
    return [];
  }
}

function currentWatchScope(): string {
  const requested = new URLSearchParams(window.location.search).get('network')?.trim().toLowerCase();
  return requested || getCurrentSite().network;
}

function emit(): void { for (const listener of listeners) listener(); }
function subscribe(listener: () => void): () => void { listeners.add(listener); return () => listeners.delete(listener); }
function persist(next: WatchEntry[]): void {
  entries = next.slice(0, 100);
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(entries)); } catch { /* in-memory fallback */ }
  emit();
}

export function toggleWatch(category: WatchCategory, id: string, label: string): void {
  const normalizedId = id.trim();
  const scope = currentWatchScope();
  const exists = entries.some((entry) => (
    entry.category === category && entry.id === normalizedId && entry.scope === scope
  ));
  persist(exists
    ? entries.filter((entry) => !(
        entry.category === category && entry.id === normalizedId && entry.scope === scope
      ))
    : [{
        category,
        id: normalizedId,
        label: label.trim() || normalizedId,
        scope,
        createdAt: new Date().toISOString(),
      }, ...entries]);
}

export function removeWatch(category: WatchCategory, id: string, scope = currentWatchScope()): void {
  persist(entries.filter((entry) => !(
    entry.category === category && entry.id === id && entry.scope === scope
  )));
}

export function useWatchlist() {
  const current = useSyncExternalStore(subscribe, () => entries);
  return {
    entries: current,
    isWatched: (category: WatchCategory, id: string) => {
      const scope = currentWatchScope();
      return current.some((entry) => (
        entry.category === category && entry.id === id && entry.scope === scope
      ));
    },
    toggle: toggleWatch,
    remove: removeWatch,
  };
}
