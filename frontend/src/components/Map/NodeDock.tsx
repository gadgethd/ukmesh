import { useEffect, useRef, useState, type ComponentProps } from 'react';
import { useWatchlist } from '../../hooks/useWatchlist.js';
import { NodePopupContent } from './NodePopupContent.js';
import { ActivitySparkline } from './ActivitySparkline.js';
import type { NodeLink, PopupNodeView } from '../../features/map/model.js';
import type { PublicScope } from '../../utils/scope.js';
import { useCommittedRef } from '../../hooks/useCommittedRef.js';

type Props = {
  nodeId: string;
  view: PopupNodeView;
  links: NodeLink[] | null;
  scope: PublicScope;
  picking: boolean;
  onClose: () => void;
  details: Omit<ComponentProps<typeof NodePopupContent>, 'props' | 'lat' | 'lon' | 'links' | 'hideName'>;
};

export function NodeDock({ nodeId, view, links, scope, picking, onClose, details }: Props) {
  const dock = useRef<HTMLElement>(null);
  const close = useCommittedRef(onClose);
  const watchlist = useWatchlist();
  const [copyLabel, setCopyLabel] = useState('Copy link');
  const copyTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const copyEpoch = useRef(0);
  useEffect(() => {
    copyEpoch.current++;
    setCopyLabel('Copy link');
    return () => { copyEpoch.current++; clearTimeout(copyTimer.current); };
  }, [nodeId]);
  useEffect(() => {
    if (picking) return;
    dock.current?.focus();
    const keydown = (event: KeyboardEvent) => { if (event.key === 'Escape') close.current(); };
    window.addEventListener('keydown', keydown);
    return () => window.removeEventListener('keydown', keydown);
  }, [nodeId, picking]);
  const name = view.props.is_prohibited ? 'Redacted node' : (view.props.name ?? `Node ${nodeId.slice(0, 8)}`);
  const status = view.props.is_stale ? 'stale' : view.props.is_online ? 'online' : 'offline';
  const observations = (links ?? []).reduce((sum, link) => sum
    + Number(link.count_this_to_peer || 0) + Number(link.count_peer_to_this || 0), 0);
  const watched = watchlist.isWatched('node', nodeId);
  const copyLink = () => {
    const epoch = copyEpoch.current;
    const done = (label: string) => {
      if (epoch !== copyEpoch.current) return;
      clearTimeout(copyTimer.current);
      setCopyLabel(label);
      copyTimer.current = setTimeout(() => setCopyLabel('Copy link'), 1600);
    };
    try { void navigator.clipboard.writeText(window.location.href).then(() => done('Copied!'), () => done('Copy failed')); }
    catch { done('Copy failed'); }
  };
  return <aside className="node-dock" aria-label="Node details" role="dialog" tabIndex={-1} ref={dock}>
    <header className="node-dock__header">
      <div className="node-dock__heading">
        <span className={`node-dock__status node-dock__status--${status}`}>{status.toUpperCase()}</span>
        <h2 className="node-dock__name" title={name}>{name}</h2>
      </div>
      <button type="button" className="node-dock__close" aria-label="Close node details" onClick={onClose}>×</button>
    </header>
    <div className="node-dock__actions">
      <button type="button" className={`node-dock__watch${watched ? ' node-dock__watch--on' : ''}`}
        onClick={() => watchlist.toggle('node', nodeId, name)}>{watched ? '★ Watching' : '☆ Watch node'}</button>
      <button type="button" className="node-dock__copy" onClick={copyLink} aria-label="Copy a link to this node">{copyLabel}</button>
    </div>
    {links !== null && links.length > 0 && <div className="node-dock__metrics">
      <div><strong>{links.length}</strong><span>neighbours</span></div>
      <div><strong>{observations.toLocaleString()}</strong><span>observations</span></div>
    </div>}
    <ActivitySparkline nodeId={nodeId} scope={scope} />
    <div className="node-dock__body"><NodePopupContent {...details} props={view.props}
      lat={view.maskedLat} lon={view.maskedLon} links={links} hideName /></div>
  </aside>;
}
