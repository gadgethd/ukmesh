import React, { memo, useEffect, useRef, useState } from 'react';

const LazyChartMount: React.FC<{ children: React.ReactNode; minHeight?: number }> = ({
  children,
  minHeight = 220,
}) => {
  const rootRef = useRef<HTMLDivElement>(null);
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    const root = rootRef.current;
    if (!root || visible) return;
    if (!('IntersectionObserver' in window)) {
      setVisible(true);
      return;
    }
    const observer = new IntersectionObserver(([entry]) => {
      if (!entry?.isIntersecting) return;
      setVisible(true);
      observer.disconnect();
    }, { rootMargin: '300px 0px' });
    observer.observe(root);
    return () => observer.disconnect();
  }, [visible]);

  return (
    <div ref={rootRef} style={{ minHeight }}>
      {visible ? children : <div className="stats-page__chart-skeleton skeleton-shimmer" aria-hidden="true" />}
    </div>
  );
};

export default memo(LazyChartMount);
