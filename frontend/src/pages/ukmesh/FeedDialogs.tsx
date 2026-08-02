import React from 'react';
import type { MeshNode } from '../../hooks/useNodes.js';
import { Dialog, DialogTitle } from '../../components/ui/Dialog.js';
import { PacketDetailPanel, type LazyPathResult } from './PacketDetailPanel.js';
import { FeedMapPanel, PacketPathTree } from './FeedPathViews.js';
import type { FeedPacket } from './feedModel.js';
import type { PathTreeStatus } from './feedState.js';
import '../path-modal.css';

export const FeedDialogs: React.FC<{
  scopeKey: string;
  packet: FeedPacket | null;
  nodeMap: Map<string, MeshNode>;
  lazyPath: LazyPathResult | null;
  pathTreeStatus: PathTreeStatus;
  pathTreeOpen: boolean;
  detailOpen: boolean;
  network: string;
  observer?: string;
  onClosePathTree: () => void;
  onCloseDetail: () => void;
  onRetryPath: () => void;
}> = ({
  scopeKey,
  packet,
  nodeMap,
  lazyPath,
  pathTreeStatus,
  pathTreeOpen,
  detailOpen,
  network,
  observer,
  onClosePathTree,
  onCloseDetail,
  onRetryPath,
}) => (
  <>
    {pathTreeOpen && packet && (
      <Dialog
        isOpen
        onOpenChange={(open) => { if (!open) onClosePathTree(); }}
        ariaLabel="Predicted repeater tree"
        overlayClassName="disclaimer-overlay"
        className="stats-page__path-modal uk-feed-path-modal"
      >
        {(close) => (
          <>
            <div className="stats-page__path-modal-header">
              <div>
                <DialogTitle className="stats-page__path-modal-title">
                  Predicted Repeater Tree
                </DialogTitle>
                <p className="stats-page__path-modal-sub">
                  {packet.packet_hash}
                  {packet.hop_count != null
                    && ` · ${packet.hop_count} hop${packet.hop_count !== 1 ? 's' : ''}`}
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
            <div className="uk-feed-path-modal__body">
              <div className="stats-page__path-modal-map">
                <FeedMapPanel
                  packet={packet}
                  nodeMap={nodeMap}
                  cachedLazyPath={lazyPath}
                  isLoading={lazyPath === null}
                />
              </div>
              <PacketPathTree
                lazyPath={lazyPath}
                nodeMap={nodeMap}
                status={lazyPath ? 'ready' : pathTreeStatus}
                onRetry={onRetryPath}
              />
            </div>
          </>
        )}
      </Dialog>
    )}
    {detailOpen && packet && (
      <Dialog
        isOpen
        onOpenChange={(open) => { if (!open) onCloseDetail(); }}
        ariaLabel="Packet details"
        overlayClassName="disclaimer-overlay"
        className="uk-feed-detail-modal"
      >
        <PacketDetailPanel
          key={`${scopeKey}:${packet.packet_hash}`}
          packet={packet}
          nodeMap={nodeMap}
          network={network}
          observer={observer}
          cachedLazyPath={lazyPath}
          onClose={onCloseDetail}
        />
      </Dialog>
    )}
  </>
);
