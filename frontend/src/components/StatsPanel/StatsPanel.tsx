import React, { useEffect, useRef, useState } from 'react';

interface StatProps {
  label: string;
  value: number;
  variant?: 'default' | 'amber' | 'online' | 'danger';
}

const AnimatedStat: React.FC<StatProps> = ({ label, value, variant = 'default' }) => {
  const [display, setDisplay] = useState(value);
  const [ticking, setTicking] = useState(false);
  const prevRef = useRef(value);

  useEffect(() => {
    if (prevRef.current === value) return;
    prevRef.current = value;
    setTicking(true);

    // Quick count-up animation
    const start = display;
    const end   = value;
    const diff  = end - start;
    if (diff === 0) return;

    const steps = Math.min(Math.abs(diff), 8);
    let step = 0;
    const interval = setInterval(() => {
      step++;
      setDisplay(Math.round(start + (diff * step) / steps));
      if (step >= steps) {
        clearInterval(interval);
        setDisplay(end);
        setTimeout(() => setTicking(false), 100);
      }
    }, 40);
    return () => clearInterval(interval);
  }, [value]); // eslint-disable-line react-hooks/exhaustive-deps

  const cls = [
    'stat__value',
    variant === 'amber'  ? 'stat__value--amber'  : '',
    variant === 'online' ? 'stat__value--online' : '',
    variant === 'danger' ? 'stat__value--danger' : '',
    ticking ? 'tick' : '',
  ].filter(Boolean).join(' ');

  return (
    <div className="stat">
      <span className="stat__label">{label}</span>
      <span className={cls}>{display.toLocaleString()}</span>
    </div>
  );
};

interface StatsPanelProps {
  mqttNodes:    number;
  mapNodes:     number;
  totalDevices: number;
  staleNodes:   number;
  packetsDay:   number;
}

export const StatsPanel: React.FC<StatsPanelProps> = ({
  mqttNodes, mapNodes, totalDevices, staleNodes, packetsDay,
}) => (
  <div className="topbar__stats">
    <AnimatedStat label="MQTT Live"     value={mqttNodes}     variant="online" />
    <div className="topbar__divider" />
    <AnimatedStat label="On Map"        value={mapNodes} />
    <div className="topbar__divider" />
    <AnimatedStat label="Total"         value={totalDevices} />
    <div className="topbar__divider" />
    <AnimatedStat label="Stale"         value={staleNodes}    variant="danger" />
    <div className="topbar__divider" />
    <AnimatedStat label="Observed / 24h" value={packetsDay}   variant="amber" />
  </div>
);
