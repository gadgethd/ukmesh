import React from 'react';

type DisclaimerModalProps = {
  onClose: () => void;
};

export const DisclaimerModal: React.FC<DisclaimerModalProps> = ({ onClose }) => (
  <div className="disclaimer-overlay" role="dialog" aria-modal="true" aria-label="Data disclaimer">
    <div className="disclaimer-modal">
      <h2 className="disclaimer-modal__title">Data disclaimer</h2>
      <div className="disclaimer-modal__body">
        <section>
          <h3>Packet paths</h3>
          <p>
            The relay paths shown on this dashboard are a best estimate. MeshCore packets include
            relay node ID hashes in the width chosen by the sending node. Depending on firmware and
            settings that can be 1, 2, or 3 bytes per hop, so path resolution matches the exact
            hash width present in the packet against known nodes. If multiple nodes still share the
            same hash at that width, the closest or strongest candidate is chosen, but the actual
            path the packet took may have been different.
          </p>
        </section>
        <section>
          <h3>Coverage map</h3>
          <p>
            The green coverage layer is a precomputed RF estimate built from terrain data and a
            simplified diffraction/path-loss model. It assumes the source repeater and the receiving
            repeater are both mounted <strong>5 metres above ground level</strong>. Actual coverage
            will still vary with local obstacles, foliage, antenna placement, and radio settings, so
            treat it as a guide rather than a guarantee of connectivity.
          </p>
        </section>
      </div>
      <button className="disclaimer-modal__close" onClick={onClose}>Got it</button>
    </div>
  </div>
);
