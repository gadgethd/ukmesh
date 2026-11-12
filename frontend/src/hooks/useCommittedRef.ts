import { useLayoutEffect, useRef } from 'react';

/** Imperative events and timers may read only the state of a committed render. */
export function useCommittedRef<Value>(value: Value) {
  const ref = useRef(value);
  useLayoutEffect(() => { ref.current = value; });
  return ref;
}
