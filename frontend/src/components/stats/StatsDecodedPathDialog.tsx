import React, { useMemo } from 'react';
import { Dialog, DialogTitle } from '../ui/Dialog.js';
import {
  DecodedPathMapView,
  type DecodedPathNode,
} from './StatsPrimitives.js';

export interface DecodedPathSelection {
  title: string;
  hash: string | null;
  hops: number | null;
  nodes: DecodedPathNode[];
}

export const StatsDecodedPathDialog: React.FC<{
  selection: DecodedPathSelection | null;
  onClose: () => void;
}> = ({ selection, onClose }) => {
  const nodes = useMemo(
    () => (selection?.nodes ?? []).filter(
      (node) => Number.isFinite(node.lat) && Number.isFinite(node.lon),
    ),
    [selection],
  );
  if (!selection || nodes.length < 2) return null;

  return (
    <Dialog
      isOpen
      onOpenChange={(open) => { if (!open) onClose(); }}
      ariaLabel="Decoded path map"
      overlayClassName="disclaimer-overlay"
      className="stats-page__path-modal"
    >
      {(close) => (
        <>
          <div className="stats-page__path-modal-header">
            <div>
              <DialogTitle className="stats-page__path-modal-title">{selection.title}</DialogTitle>
              <p className="stats-page__path-modal-sub">
                {selection.hash} · {selection.hops ?? 0} hops
              </p>
            </div>
            <button
              type="button"
              className="disclaimer-modal__close stats-page__path-modal-close"
              onClick={close}
            >
              Close
            </button>
          </div>
          <div className="stats-page__path-modal-map">
            <DecodedPathMapView nodes={nodes} />
          </div>
          <div className="stats-page__path-modal-list">
            {nodes.map((node) => (
              <div key={`${node.node_id}-label-${node.ord}`} className="stats-page__path-modal-node">
                <span>{node.ord}</span>
                <strong>
                  {node.name ?? node.node_id}
                  {node.name === 'Redacted repeater' ? ' · approximate within 1 mile' : ''}
                </strong>
              </div>
            ))}
          </div>
        </>
      )}
    </Dialog>
  );
};
