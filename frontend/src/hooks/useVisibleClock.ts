import { useEffect, useState } from 'react';
import { POLLING } from '../config/polling.js';

/** One clock per mounted status view; hidden documents do not schedule ticks. */
export function useVisibleClock(interval: number = POLLING.feedClock) {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    let timer: number | undefined;
    const update = () => {
      window.clearTimeout(timer);
      if (document.visibilityState !== 'visible') return;
      setNow(Date.now());
      timer = window.setTimeout(update, interval);
    };
    document.addEventListener('visibilitychange', update);
    update();
    return () => {
      window.clearTimeout(timer);
      document.removeEventListener('visibilitychange', update);
    };
  }, [interval]);
  return now;
}
