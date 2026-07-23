import React, { useEffect, useMemo, useState } from 'react';
import { useOverlayStore } from '../../store/overlayStore.js';
import { withScopeParams } from '../../utils/api.js';

type Bucket = {
  time: string;
  packetCount: number;
  observerCount: number;
  activeNodeIds: string[];
};

type Payload = {
  generatedAt: string;
  windowMinutes: number;
  bucketMinutes: number;
  buckets: Bucket[];
};

type Props = { network?: string; observer?: string };

export const TimelineControl: React.FC<Props> = ({ network, observer }) => {
  const [payload, setPayload] = useState<Payload | null>(null);
  const [index, setIndex] = useState(-1);
  const [open, setOpen] = useState(false);
  const setReplayBucket = useOverlayStore((state) => state.setReplayBucket);
  const clearReplay = useOverlayStore((state) => state.clearReplay);

  useEffect(() => {
    const controller = new AbortController();
    fetch(withScopeParams('/api/activity/timeline?minutes=360&bucket=15', { network, observer }), { signal: controller.signal })
      .then((response) => response.ok ? response.json() as Promise<Payload> : Promise.reject(new Error('timeline unavailable')))
      .then((next) => {
        setPayload(next);
        const replay = new URLSearchParams(window.location.search).get('replay');
        const restoredIndex = replay ? next.buckets.findIndex((bucket) => bucket.time === replay) : -1;
        if (restoredIndex >= 0) {
          setIndex(restoredIndex);
          setOpen(true);
          setReplayBucket(next.buckets[restoredIndex]!.time, next.buckets[restoredIndex]!.activeNodeIds);
        } else {
          setIndex(next.buckets.length);
        }
      })
      .catch(() => setPayload(null));
    return () => controller.abort();
  }, [network, observer, setReplayBucket]);

  useEffect(() => () => clearReplay(), [clearReplay]);

  const activeBucket = payload && index >= 0 && index < payload.buckets.length ? payload.buckets[index]! : null;
  const label = useMemo(() => {
    if (!activeBucket) return 'Live';
    return new Date(activeBucket.time).toLocaleString([], { weekday: 'short', hour: '2-digit', minute: '2-digit' });
  }, [activeBucket]);

  const selectIndex = (nextIndex: number) => {
    if (!payload) return;
    setIndex(nextIndex);
    const url = new URL(window.location.href);
    const bucket = payload.buckets[nextIndex];
    if (bucket) {
      setReplayBucket(bucket.time, bucket.activeNodeIds);
      url.searchParams.set('replay', bucket.time);
    } else {
      clearReplay();
      url.searchParams.delete('replay');
    }
    window.history.replaceState(null, '', url);
  };

  if (!payload || payload.buckets.length === 0) return null;
  return (
    <section className={`timeline-control${open ? ' timeline-control--open' : ''}`} aria-label="Network activity replay">
      <button type="button" className="timeline-control__toggle" onClick={() => setOpen((value) => !value)} aria-expanded={open}>
        <span>Activity replay</span><strong>{label}</strong>
      </button>
      {open && (
        <div className="timeline-control__body">
          <input
            type="range"
            min={0}
            max={payload.buckets.length}
            step={1}
            value={index < 0 ? payload.buckets.length : index}
            onChange={(event) => selectIndex(Number(event.target.value))}
            aria-label="Replay time"
          />
          <div className="timeline-control__meta" aria-live="polite">
            {activeBucket ? (
              <><span>{activeBucket.packetCount.toLocaleString()} packets</span><span>{activeBucket.observerCount} observers</span><span>{activeBucket.activeNodeIds.length} active nodes shown</span></>
            ) : <span>Showing current live state</span>}
          </div>
          <p>Historical buckets are 15-minute aggregates. Highlighted nodes were the most active; current positions are used.</p>
        </div>
      )}
    </section>
  );
};
