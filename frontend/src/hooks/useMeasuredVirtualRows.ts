import { useCallback, useLayoutEffect, useMemo, useRef, useState, type RefCallback, type RefObject } from 'react';
import { measuredRowRange, rowAtOffset, rowOffsets } from './measuredVirtualRows.js';

function readViewport(container: HTMLDivElement) {
  const internal = container.scrollHeight > container.clientHeight + 1;
  const rect = container.getBoundingClientRect();
  return {
    internal,
    scrollTop: internal ? container.scrollTop : Math.max(0, -rect.top),
    height: internal ? container.clientHeight : Math.max(0, Math.min(window.innerHeight, rect.bottom) - Math.max(0, rect.top)),
  };
}

function writeScroll(container: HTMLDivElement, position: number) {
  if (readViewport(container).internal) container.scrollTop = position;
  else window.scrollTo({ top: window.scrollY + container.getBoundingClientRect().top + position, behavior: 'instant' });
}

/** Natural-height rows with stable identities; the estimate is only for unseen rows. */
export function useMeasuredVirtualRows(keys: readonly string[], containerRef: RefObject<HTMLDivElement | null>) {
  const heights = useRef(new Map<string, number>());
  const elements = useRef(new Map<string, HTMLElement>());
  const callbacks = useRef(new Map<string, RefCallback<HTMLElement>>());
  const observer = useRef<ResizeObserver | null>(null);
  const width = useRef(0);
  const scroll = useRef(0);
  const [revision, setRevision] = useState(0);
  const [viewport, setViewport] = useState({ scrollTop: 0, height: 0 });
  const offsets = useMemo(() => rowOffsets(keys, heights.current), [keys, revision]);
  const previous = useRef({ keys, offsets });

  const updateViewport = useCallback(() => {
    const container = containerRef.current;
    if (!container) return;
    const { scrollTop, height } = readViewport(container);
    scroll.current = scrollTop;
    setViewport(current => current.scrollTop === scrollTop && current.height === height
      ? current : { scrollTop, height });
  }, [containerRef]);

  const measure = useCallback(() => {
    const container = containerRef.current;
    if (!container) return;
    let changed = false;
    // Offscreen measurements are invalid after a width change (wrapping).
    if (width.current !== container.clientWidth) {
      width.current = container.clientWidth;
      heights.current.clear();
      changed = true;
    }
    for (const [key, element] of elements.current) {
      const height = element.getBoundingClientRect().height;
      if (height > 0 && heights.current.get(key) !== height) {
        heights.current.set(key, height);
        changed = true;
      }
    }
    if (changed) setRevision(value => value + 1);
    // Read height here, but leave scroll anchoring to the layout effect below.
    const { height } = readViewport(container);
    setViewport(current => current.height === height ? current : { ...current, height });
  }, [containerRef]);

  const measureRow = useCallback((key: string): RefCallback<HTMLElement> => {
    let callback = callbacks.current.get(key);
    if (!callback) {
      callback = element => {
        const old = elements.current.get(key);
        if (old) observer.current?.unobserve(old);
        if (element) {
          elements.current.set(key, element);
          observer.current?.observe(element);
        } else elements.current.delete(key);
      };
      callbacks.current.set(key, callback);
    }
    return callback;
  }, []);

  useLayoutEffect(() => {
    const resize = new ResizeObserver(measure);
    observer.current = resize;
    if (containerRef.current) resize.observe(containerRef.current);
    for (const element of elements.current.values()) resize.observe(element);
    // Mobile CSS deliberately lets the document scroll. Its viewport is the
    // visible intersection with the list, not the list's full content height.
    window.addEventListener('scroll', updateViewport, { passive: true });
    window.addEventListener('resize', measure);
    return () => {
      resize.disconnect(); observer.current = null;
      window.removeEventListener('scroll', updateViewport);
      window.removeEventListener('resize', measure);
    };
  }, [containerRef, measure, updateViewport]);

  useLayoutEffect(() => {
    const container = containerRef.current;
    const old = previous.current;
    if (container && old.offsets !== offsets) {
      // Preserve the first visible packet when tags resize earlier rows or a
      // new packet is prepended. Keep a feed at the top pinned to live traffic.
      const oldIndex = rowAtOffset(old.offsets, scroll.current);
      const anchor = old.keys[oldIndex];
      const newIndex = anchor === undefined ? -1 : keys.indexOf(anchor);
      if (scroll.current > 1) {
        const atBottom = scroll.current + viewport.height >= old.offsets[old.offsets.length - 1]! - 1;
        const position = newIndex < 0 ? 0 : atBottom
          ? offsets[offsets.length - 1]! - viewport.height
          : offsets[newIndex]! + Math.min(scroll.current - old.offsets[oldIndex]!, offsets[newIndex + 1]! - offsets[newIndex]! - 1);
        writeScroll(container, position);
      }
    }
    previous.current = { keys, offsets };
    const retained = new Set(keys);
    for (const key of heights.current.keys()) if (!retained.has(key)) heights.current.delete(key);
    for (const key of callbacks.current.keys()) if (!retained.has(key)) callbacks.current.delete(key);
    updateViewport();
    // Measure before paint as well as on ResizeObserver notifications, so new
    // packets and asynchronous tag/observer updates have correct spacers.
    measure();
  });

  return { ...measuredRowRange(offsets, viewport.scrollTop, viewport.height), measureRow, onScroll: updateViewport };
}
