import {
  useCallback,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type RefCallback,
  type RefObject,
} from 'react';
import { measuredRowRange, rowAtOffset, rowOffsets } from './measuredVirtualRows.js';

function readViewport(container: HTMLDivElement) {
  const internal = container.scrollHeight > container.clientHeight + 1;
  const rect = container.getBoundingClientRect();
  return {
    internal,
    scrollTop: internal ? container.scrollTop : Math.max(0, -rect.top),
    height: internal
      ? container.clientHeight
      : Math.max(0, Math.min(window.innerHeight, rect.bottom) - Math.max(0, rect.top)),
  };
}

function writeScroll(container: HTMLDivElement, position: number) {
  if (readViewport(container).internal) {
    container.scrollTop = position;
  } else {
    window.scrollTo({
      top: window.scrollY + container.getBoundingClientRect().top + position,
      behavior: 'instant',
    });
  }
}

/** Natural-height rows with stable identities; the estimate is only for unseen rows. */
export function useMeasuredVirtualRows(
  keys: readonly string[],
  containerRef: RefObject<HTMLDivElement | null>,
) {
  const heights = useRef(new Map<string, number>());
  const elements = useRef(new Map<string, HTMLElement>());
  const callbacks = useRef(new Map<string, RefCallback<HTMLElement>>());
  const observer = useRef<ResizeObserver | null>(null);
  const width = useRef(0);
  const scroll = useRef(0);
  const mode = useRef<boolean | undefined>(undefined);
  const [revision, setRevision] = useState(0);
  const [viewport, setViewport] = useState({ scrollTop: 0, height: 0 });
  const offsets = useMemo(() => rowOffsets(keys, heights.current), [keys, revision]);
  const previous = useRef({ keys, offsets });

  const updateViewport = useCallback(() => {
    const container = containerRef.current;
    if (!container) return;
    const { internal, scrollTop, height } = readViewport(container);
    const switched = mode.current !== undefined && mode.current !== internal;
    mode.current = internal;
    // A scroll-mode flip (internal container <-> document scrolling) resets the
    // raw reading before React commits its new row range. Under React 19.3 the
    // window scroll listener can fire before that commit, so the anchor effect
    // would see 0 and drop the position. Transfer the last known content offset
    // so the same packet stays visible across the flip.
    if (switched && scroll.current > 1 && scrollTop < 1) {
      writeScroll(container, scroll.current);
      scroll.current = readViewport(container).scrollTop;
    } else {
      scroll.current = scrollTop;
    }
    setViewport((current) => current.scrollTop === scroll.current && current.height === height
      ? current
      : { scrollTop: scroll.current, height });
  }, [containerRef]);

  const measure = useCallback(() => {
    const container = containerRef.current;
    if (!container) return;
    let changed = false;
    // Offscreen measurements are invalid after a width change (text wrapping).
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
    if (changed) setRevision((value) => value + 1);
    const { height } = readViewport(container);
    setViewport((current) => current.height === height ? current : { ...current, height });
  }, [containerRef]);

  const measureRow = useCallback((key: string): RefCallback<HTMLElement> => {
    let callback = callbacks.current.get(key);
    if (!callback) {
      callback = (element) => {
        const old = elements.current.get(key);
        if (old) observer.current?.unobserve(old);
        if (element) {
          elements.current.set(key, element);
          observer.current?.observe(element);
        } else {
          elements.current.delete(key);
        }
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
    // On small layouts the document can scroll while the virtual list does not.
    window.addEventListener('scroll', updateViewport, { passive: true });
    window.addEventListener('resize', measure);
    return () => {
      resize.disconnect();
      observer.current = null;
      window.removeEventListener('scroll', updateViewport);
      window.removeEventListener('resize', measure);
    };
  }, [containerRef, measure, updateViewport]);

  useLayoutEffect(() => {
    const container = containerRef.current;
    const old = previous.current;
    if (container && old.offsets !== offsets) {
      // Preserve the first visible packet through prepends and row remeasurement.
      const oldIndex = rowAtOffset(old.offsets, scroll.current);
      const anchor = old.keys[oldIndex];
      const newIndex = anchor === undefined ? -1 : keys.indexOf(anchor);
      if (scroll.current > 1) {
        const oldTotal = old.offsets[old.offsets.length - 1]!;
        const newTotal = offsets[offsets.length - 1]!;
        const atBottom = scroll.current + viewport.height >= oldTotal - 1;
        const position = newIndex < 0
          ? 0
          : atBottom
            ? newTotal - viewport.height
            : offsets[newIndex]!
              + Math.min(
                scroll.current - old.offsets[oldIndex]!,
                offsets[newIndex + 1]! - offsets[newIndex]! - 1,
              );
        writeScroll(container, Math.max(0, position));
      }
    }
    previous.current = { keys, offsets };
    const retained = new Set(keys);
    for (const key of heights.current.keys()) {
      if (!retained.has(key)) heights.current.delete(key);
    }
    for (const key of callbacks.current.keys()) {
      if (!retained.has(key)) callbacks.current.delete(key);
    }
    updateViewport();
    // Measure before paint and on ResizeObserver notifications so spacers follow content.
    measure();
  });

  return {
    ...measuredRowRange(offsets, viewport.scrollTop, viewport.height),
    measureRow,
    onScroll: updateViewport,
  };
}
