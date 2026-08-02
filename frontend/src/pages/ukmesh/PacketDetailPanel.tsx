import React, { useMemo } from 'react';
import { LoadingIndicator } from '../../components/LoadingIndicator.js';
import type { MeshNode } from '../../hooks/useNodes.js';
import type { FeedPacket } from './UKFeedPage.js';
import { usePacketDetailData, type RadioState } from '../../hooks/usePacketDetailData.js';
import {
  PathMap,
  type LazyPath,
  type LazyPathNode,
  type LazyPathResult,
  type ResolvedPath,
} from './PacketPathMap.js';

export { PathMap };
export type { LazyPath, LazyPathNode, LazyPathResult, ResolvedPath };

// ── Types ─────────────────────────────────────────────────────────────────────

// ── Constants ─────────────────────────────────────────────────────────────────

const PAYLOAD_NAMES: Record<number, string> = {
  0: 'Request', 1: 'Response', 2: 'TextMessage', 3: 'Ack',
  4: 'Advertisement', 5: 'GroupMessage', 6: 'Data', 7: 'Anon',
  8: 'Path', 9: 'Trace', 11: 'Control',
};

const ROUTE_NAMES: Record<number, string> = {
  0: 'Transport Flood', 1: 'Flood', 2: 'Direct', 3: 'Transport Direct',
};
const LAZY_PATH_COLORS = ['#26c6a2', '#00b4d8', '#f59e0b', '#a78bfa', '#f87171'];

// ── Hex parsing ───────────────────────────────────────────────────────────────

function hexToBytes(hex: string): Uint8Array {
  const clean = hex.replace(/\s/g, '');
  const bytes = new Uint8Array(Math.floor(clean.length / 2));
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

function hexSlice(hex: string, byteStart: number, byteEnd: number): string {
  return hex.slice(byteStart * 2, byteEnd * 2).toUpperCase();
}

function byteRangeName(start: number, end: number): string {
  return start === end - 1 ? `Byte ${start}` : `Bytes ${start}–${end - 1}`;
}

type ParsedPacket = {
  totalBytes: number;
  headerByte: number;
  rawRouteType: number;   // bits 0-1
  rawPayloadType: number; // bits 2-5
  version: number;        // bits 6-7
  hasTransportCodes: boolean;
  transportCodesHex: string | null;
  pathLengthByte: number;
  pathHashCount: number;
  pathHashSizeBytes: number;
  pathDataStart: number;
  pathDataEnd: number;
  payloadStart: number;
};

function parsePacketHex(hex: string): ParsedPacket | null {
  const clean = hex.replace(/\s/g, '');
  if (clean.length < 4) return null;
  const bytes = hexToBytes(clean);
  if (bytes.length < 2) return null;

  const headerByte = bytes[0]!;
  const rawRouteType = headerByte & 0x03;
  const rawPayloadType = (headerByte >> 2) & 0x0F;
  const version = (headerByte >> 6) & 0x03;

  // routeType 0 or 3 have 4 transport code bytes before path length
  const hasTransportCodes = rawRouteType === 0 || rawRouteType === 3;
  const lengthByteOffset = hasTransportCodes ? 5 : 1;

  if (bytes.length <= lengthByteOffset) return null;
  const pathLengthByte = bytes[lengthByteOffset]!;
  const pathHashCount = pathLengthByte & 0x3f;
  const pathHashSizeBytes = (pathLengthByte >> 6) + 1;

  const pathDataStart = lengthByteOffset + 1;
  const pathDataEnd = pathDataStart + pathHashCount * pathHashSizeBytes;
  const payloadStart = pathDataEnd;

  return {
    totalBytes: bytes.length,
    headerByte,
    rawRouteType,
    rawPayloadType,
    version,
    hasTransportCodes,
    transportCodesHex: hasTransportCodes ? hexSlice(clean, 1, 5) : null,
    pathLengthByte,
    pathHashCount,
    pathHashSizeBytes,
    pathDataStart,
    pathDataEnd,
    payloadStart,
  };
}

// ── Byte breakdown section ────────────────────────────────────────────────────

const ByteSection: React.FC<{
  label: string;
  byteRange: string;
  hexValue: string;
  description?: string;
  children?: React.ReactNode;
}> = ({ label, byteRange, hexValue, description, children }) => (
  <div className="feed-detail__byte-section">
    <div className="feed-detail__byte-section-header">
      <span className="feed-detail__byte-label">{label}</span>
      <span className="feed-detail__byte-name">{byteRange}</span>
      <code className="feed-detail__byte-label" style={{ background: 'transparent', color: 'var(--text-secondary)' }}>{hexValue}</code>
    </div>
    {(description || children) && (
      <div className="feed-detail__byte-body">
        {description && <p className="feed-detail__byte-note">{description}</p>}
        {children}
      </div>
    )}
  </div>
);

const BitTable: React.FC<{
  rows: Array<{ bits: string; field: string; value: string; binary: string }>;
}> = ({ rows }) => (
  <table className="feed-detail__bit-table">
    <thead>
      <tr><th scope="col">Bits</th><th scope="col">Field</th><th scope="col">Value</th><th scope="col">Binary</th></tr>
    </thead>
    <tbody>
      {rows.map((row, i) => (
        <tr key={i}>
          <td>{row.bits}</td><td>{row.field}</td><td>{row.value}</td><td style={{ fontFamily: 'var(--font-mono)', fontSize: '10px', color: 'var(--text-muted)' }}>{row.binary}</td>
        </tr>
      ))}
    </tbody>
  </table>
);

// ── Radio formatting ──────────────────────────────────────────────────────────

function formatRadio(radio: RadioState): string {
  const parts: string[] = [];
  if (radio.frequency != null) parts.push(`${(radio.frequency / 1_000_000).toFixed(3)} MHz`);
  if (radio.sf != null) parts.push(`SF${radio.sf}`);
  if (radio.bw != null) parts.push(`BW${radio.bw >= 1000 ? (radio.bw / 1000).toFixed(1) : radio.bw}`);
  if (radio.cr != null) parts.push(`CR${radio.cr}`);
  return parts.join(' / ') || '—';
}

// ── Main panel ────────────────────────────────────────────────────────────────

const TYPE_LABELS: Record<number, string> = {
  0: 'REQ', 1: 'RSP', 2: 'DM', 3: 'ACK', 4: 'ADV', 5: 'GRP',
  6: 'DAT', 7: 'ANON', 8: 'PATH', 9: 'TRC', 11: 'CTL',
};


export const PacketDetailPanel: React.FC<{
  packet: FeedPacket;
  nodeMap: Map<string, MeshNode>;
  network: string;
  observer?: string;
  onClose: () => void;
  cachedLazyPath?: LazyPathResult | null;
}> = ({ packet, nodeMap, network, observer, onClose, cachedLazyPath }) => {
  const observerKey = (packet.observer_node_ids ?? []).slice().sort().join(',');
  const { detail, resolvedPaths, radio, loading, pathLoading, lazyPath, lazyStatus, lazyCountdown } = usePacketDetailData({
    packetHash: packet.packet_hash,
    network,
    observer,
    observerKey,
    hasPathHashes: Boolean(packet.path_hashes?.length),
    cachedLazyPath,
  });

  // Observer info
  const rxNodeId = detail?.rxNodeId ?? packet.rx_node_id ?? null;
  const rxNode = rxNodeId ? nodeMap.get(rxNodeId) : undefined;
  const observerName = rxNode?.name ?? rxNodeId?.slice(0, 8) ?? '—';
  const observerIata = rxNode?.iata?.trim().toUpperCase() ?? '—';

  // Regions heard — combine live observer_node_ids, rx_node_id fallback, and DB observations
  const regionsHeard = useMemo(() => {
    const iatas = new Set<string>();
    const ids: (string | null | undefined)[] = [
      ...(packet.observer_node_ids?.length ? packet.observer_node_ids : [packet.rx_node_id]),
      ...(detail?.observations?.map((o) => o.rxNodeId) ?? []),
    ];
    for (const id of ids) {
      if (!id) continue;
      const iata = nodeMap.get(id)?.iata;
      if (iata) iatas.add(iata.trim().toUpperCase());
    }
    return Array.from(iatas).join(' · ') || '—';
  }, [packet.observer_node_ids, packet.rx_node_id, detail?.observations, nodeMap]);

  // Propagation time — span from first observer to last observer receiving this packet
  const propagationTime = useMemo(() => {
    if (!detail?.observations || detail.observations.length < 2) return null;
    const times = detail.observations.map((o) => Date.parse(o.time)).filter(Number.isFinite);
    if (times.length < 2) return null;
    const diffMs = Math.max(...times) - Math.min(...times);
    if (diffMs === 0) return null; // all observers at same ms
    if (diffMs > 300_000) return null; // >5min likely clock skew, not propagation
    return diffMs < 1000 ? `${diffMs}ms` : `${(diffMs / 1000).toFixed(2)}s`;
  }, [detail]);

  // Path text
  const pathText = useMemo(() => {
    const hashes = packet.path_hashes ?? detail?.pathHashes;
    if (!hashes?.length) return null;
    return hashes.map((h) => h.toUpperCase()).join('→');
  }, [packet.path_hashes, detail?.pathHashes]);

  // Byte breakdown
  const parsed = useMemo(() => {
    if (!detail?.rawHex) return null;
    return parsePacketHex(detail.rawHex);
  }, [detail?.rawHex]);

  const routeLabel = detail?.routeType != null ? (ROUTE_NAMES[detail.routeType] ?? `Type${detail.routeType}`) : '—';
  const typeLabel = packet.packet_type != null ? (TYPE_LABELS[packet.packet_type] ?? `T${packet.packet_type}`) : '—';
  const heardAt = new Date(packet.time).toLocaleString();
  const observerCount = detail?.observations?.length ?? packet.rx_count ?? 1;

  const resolvedHopCount = useMemo(() => {
    const allNodes = new Set<string>();
    for (const r of resolvedPaths) {
      r.purplePath?.forEach(([lat, lon]) => allNodes.add(`${lat},${lon}`));
    }
    return allNodes.size;
  }, [resolvedPaths]);

  const totalHops = packet.path_hashes?.length ?? packet.hop_count ?? null;

  // Observer GPS positions for map markers
  const observerPositions = useMemo((): [number, number][] => {
    const allIds = new Set<string>();
    const candidates: string[] = [
      ...(packet.observer_node_ids?.length ? packet.observer_node_ids : [packet.rx_node_id].filter(Boolean) as string[]),
      ...(detail?.observations?.map((o) => o.rxNodeId).filter(Boolean) as string[] ?? []),
    ];
    const positions: [number, number][] = [];
    for (const id of candidates) {
      if (!id || allIds.has(id)) continue;
      allIds.add(id);
      const node = nodeMap.get(id);
      if (node?.lat != null && node?.lon != null) {
        positions.push([node.lat, node.lon]);
      }
    }
    return positions;
  }, [packet.observer_node_ids, packet.rx_node_id, detail?.observations, nodeMap]);

  return (
    <div className="feed-detail-panel">
      {/* Header */}
      <div className="feed-detail__header">
        <code className="feed-detail__hash">{packet.packet_hash}</code>
        <span className="feed-detail__badge">{typeLabel}</span>
        {totalHops != null && <span className="feed-detail__badge feed-detail__badge--muted">{totalHops} hop{totalHops !== 1 ? 's' : ''}</span>}
        <button type="button" className="feed-detail__close" onClick={onClose} aria-label="Close packet details">✕</button>
      </div>

      {loading && (
        <div className="feed-detail__loading">
          <LoadingIndicator label="Loading packet details..." variant="inline" />
        </div>
      )}

      {/* Info grid */}
      <div className="feed-detail__section">
        <div className="feed-detail__info-grid">
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Observer</span>
            <span className="feed-detail__info-value">{observerName}</span>
          </div>
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Route</span>
            <span className="feed-detail__info-value">{routeLabel}</span>
          </div>
          {radio && (
            <div className="feed-detail__info-item">
              <span className="feed-detail__info-label">Radio</span>
              <span className="feed-detail__info-value">{formatRadio(radio)}</span>
            </div>
          )}
          {propagationTime && (
            <div className="feed-detail__info-item">
              <span className="feed-detail__info-label">Propagation</span>
              <span className="feed-detail__info-value">{propagationTime}</span>
            </div>
          )}
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Heard at</span>
            <span className="feed-detail__info-value">{heardAt}</span>
          </div>
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Heard by</span>
            <span className="feed-detail__info-value">{observerCount} observer{observerCount !== 1 ? 's' : ''}</span>
          </div>
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Observer region</span>
            <span className="feed-detail__info-value">{observerIata}</span>
          </div>
          <div className="feed-detail__info-item">
            <span className="feed-detail__info-label">Regions heard</span>
            <span className="feed-detail__info-value">{regionsHeard}</span>
          </div>
        </div>
      </div>

      {/* Observer table */}
      {detail?.observations && detail.observations.length > 0 && (
        <div className="feed-detail__section">
          <div className="feed-detail__section-title">Observers ({detail.observations.length})</div>
          <table className="feed-detail__observer-table">
            <thead>
              <tr><th scope="col">Node</th><th scope="col">Region</th><th scope="col">Hops</th><th scope="col">RSSI</th><th scope="col">SNR</th><th scope="col">Time</th></tr>
            </thead>
            <tbody>
              {detail.observations.map((obs, i) => {
                const node = obs.rxNodeId ? nodeMap.get(obs.rxNodeId) : undefined;
                const iata = node?.iata?.trim().toUpperCase() ?? '—';
                const name = node?.name ?? (obs.rxNodeId ? `${obs.rxNodeId.slice(0, 8)}…` : '—');
                return (
                  <tr key={i}>
                    <td>{name}</td>
                    <td>{iata}</td>
                    <td>{obs.hopCount ?? '—'}</td>
                    <td>{obs.rssi ?? '—'}</td>
                    <td>{obs.snr != null ? obs.snr.toFixed(1) : '—'}</td>
                    <td>{new Date(obs.time).toLocaleTimeString()}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Lazy path status + hop table */}
      <div className="feed-detail__section">
        <div className="feed-detail__section-title">
          Hash-traced path
          {lazyStatus === 'settling' && (
            <span className="feed-detail__section-note"> — settling ({lazyCountdown}s)</span>
          )}
          {lazyStatus === 'loading' && (
            <span className="feed-detail__section-note">
              {' '}
              <LoadingIndicator label="Resolving..." variant="inline" />
            </span>
          )}
          {lazyStatus === 'done' && lazyPath && (() => {
            const totalMatched = lazyPath.paths.reduce((s, p) => s + p.matchedHops, 0);
            const totalHopsAll = lazyPath.paths.reduce((s, p) => s + p.totalHops, 0);
            return (
              <span className="feed-detail__section-note">
                {' '}— {totalMatched} of {totalHopsAll} hops matched
                {lazyPath.paths.length > 1 && ` (${lazyPath.paths.length} paths)`}
                {totalMatched > 0 && <span className="feed-detail__lazy-dot" />}
              </span>
            );
          })()}
          {lazyStatus === 'notfound' && (
            <span className="feed-detail__section-note"> — no path hashes</span>
          )}
          {lazyStatus === 'error' && (
            <span className="feed-detail__section-note feed-detail__section-note--error"> — fetch failed</span>
          )}
        </div>
        {lazyPath && lazyPath.paths.map((lp, pi) => (
          lp.canonicalPath.length > 0 && (
            <React.Fragment key={pi}>
              {lazyPath.paths.length > 1 && (
                <div style={{ fontSize: '11px', color: LAZY_PATH_COLORS[pi % LAZY_PATH_COLORS.length], marginBottom: '4px', marginTop: pi > 0 ? '8px' : undefined }}>
                  Path {pi + 1} — {lp.observerIds.length} observer{lp.observerIds.length !== 1 ? 's' : ''}
                </div>
              )}
              <table className="feed-detail__observer-table">
                <thead>
                  <tr><th scope="col">Hop</th><th scope="col">Hash</th><th scope="col">Node</th><th scope="col">Region</th><th scope="col">Seen by</th></tr>
                </thead>
                <tbody>
                  {lp.canonicalPath.map((step, si) => {
                    const iata = step.nodeId ? nodeMap.get(step.nodeId)?.iata?.trim().toUpperCase() ?? '—' : '—';
                    const matched = step.nodeId !== null && !step.ambiguous;
                    const rowClass = step.isObserver
                      ? 'feed-detail__lazy-row--observer'
                      : matched ? 'feed-detail__lazy-row--matched' : 'feed-detail__lazy-row--unmatched';
                    return (
                      <tr key={`${step.position}-${si}`} className={rowClass}>
                        <td>{step.isObserver ? '▶' : step.position + 1}</td>
                        <td><code style={{ fontSize: '10px', color: 'var(--text-muted)' }}>{step.isObserver ? 'observer' : step.hash}</code></td>
                        <td>{step.isObserver ? `[${step.name ?? step.nodeId?.slice(0, 10) ?? '?'}]` : step.ambiguous ? `${step.name ?? step.nodeId?.slice(0, 10)} (+amb)` : (step.name ?? step.nodeId?.slice(0, 10) ?? '—')}</td>
                        <td>{iata}</td>
                        <td style={{ color: 'var(--text-muted)' }}>{step.isObserver ? '—' : `${step.appearances}/${step.totalObservations}`}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </React.Fragment>
          )
        ))}
        {lazyStatus === 'settling' && (
          <div className="feed-detail__lazy-hint">
            Waiting for propagation to settle before tracing route from packet hashes…
          </div>
        )}
      </div>

      {/* Path text */}
      {pathText && (
        <div className="feed-detail__section">
          <div className="feed-detail__section-title">Path ({totalHops ?? '?'} hops)</div>
          <div className="feed-detail__path-text">{pathText}</div>
        </div>
      )}

      {/* Map */}
      <div className="feed-detail__section">
        <div className="feed-detail__section-title">
          Resolved path
          {pathLoading && (
            <span className="feed-detail__section-note">
              {' '}
              <LoadingIndicator label="Updating..." variant="inline" />
            </span>
          )}
          {!pathLoading && totalHops != null && resolvedHopCount > 0 && (
            <span className="feed-detail__section-note"> — {resolvedHopCount} of {totalHops} hops located</span>
          )}
        </div>
        <div className="feed-detail__map">
          {!loading && (
            <PathMap
              key={packet.packet_hash}
              results={resolvedPaths}
              observerPositions={observerPositions}
              lazyPaths={lazyPath?.paths ?? []}
              nodeMap={nodeMap}
              isLoading={lazyStatus === 'settling' || lazyStatus === 'loading'}
            />
          )}
        </div>
      </div>

      {/* Raw hex */}
      {detail?.rawHex && (
        <div className="feed-detail__section">
          <div className="feed-detail__section-title">
            Packet Byte Breakdown ({Math.floor(detail.rawHex.replace(/\s/g, '').length / 2)} bytes)
          </div>
          <code className="feed-detail__hex">{detail.rawHex.toUpperCase()}</code>

          {parsed && (
            <div className="feed-detail__breakdown">
              {/* Header byte */}
              <ByteSection
                label="Header"
                byteRange={byteRangeName(0, 1)}
                hexValue={`0x${detail.rawHex.slice(0, 2).toUpperCase()}`}
              >
                <BitTable rows={[
                  { bits: '0–1', field: 'Route Type', value: ROUTE_NAMES[parsed.rawRouteType] ?? `${parsed.rawRouteType}`, binary: parsed.rawRouteType.toString(2).padStart(2, '0') },
                  { bits: '2–5', field: 'Payload Type', value: PAYLOAD_NAMES[parsed.rawPayloadType] ?? `${parsed.rawPayloadType}`, binary: parsed.rawPayloadType.toString(2).padStart(4, '0') },
                  { bits: '6–7', field: 'Version', value: String(parsed.version), binary: parsed.version.toString(2).padStart(2, '0') },
                ]} />
              </ByteSection>

              {/* Transport codes (if present) */}
              {parsed.hasTransportCodes && parsed.transportCodesHex && (
                <ByteSection
                  label="Transport Codes"
                  byteRange={byteRangeName(1, 5)}
                  hexValue={parsed.transportCodesHex}
                  description="4-byte transport codes"
                />
              )}

              {/* Path length */}
              <ByteSection
                label="Path Length"
                byteRange={byteRangeName(parsed.hasTransportCodes ? 5 : 1, (parsed.hasTransportCodes ? 5 : 1) + 1)}
                hexValue={`0x${detail.rawHex.slice((parsed.hasTransportCodes ? 5 : 1) * 2, (parsed.hasTransportCodes ? 5 : 1) * 2 + 2).toUpperCase()}`}
                description={`${parsed.pathHashCount} × ${parsed.pathHashSizeBytes}-byte hash${parsed.pathHashCount !== 1 ? 'es' : ''} showing route taken`}
              />

              {/* Path data */}
              {parsed.pathHashCount > 0 && (
                <ByteSection
                  label="Path Data"
                  byteRange={byteRangeName(parsed.pathDataStart, parsed.pathDataEnd)}
                  hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.pathDataStart, parsed.pathDataEnd)}
                  description="Historical route taken (bytes added as packet floods)"
                />
              )}

              {/* Payload */}
              {parsed.payloadStart < parsed.totalBytes && (
                <ByteSection
                  label={`Payload — ${PAYLOAD_NAMES[parsed.rawPayloadType] ?? `Type ${parsed.rawPayloadType}`}`}
                  byteRange={byteRangeName(parsed.payloadStart, parsed.totalBytes)}
                  hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.payloadStart, parsed.totalBytes)}
                  description={`${PAYLOAD_NAMES[parsed.rawPayloadType] ?? 'Unknown'} payload data`}
                >
                  {/* Advertisement decode */}
                  {parsed.rawPayloadType === 4 && detail?.payload && (
                    <div className="feed-detail__adv-fields">
                      {(() => {
                        const app = (detail.payload['appData'] ?? detail.payload) as Record<string, unknown>;
                        const rows: Array<[string, string]> = [];
                        if (typeof app['name'] === 'string') rows.push(['Name', app['name']]);
                        if (typeof app['role'] === 'number') rows.push(['Role', ['?', 'ChatNode', 'Repeater', 'RoomServer', 'Sensor'][app['role'] as number] ?? `${app['role']}`]);
                        if (typeof app['lat'] === 'number' && typeof app['lon'] === 'number') rows.push(['Location', `${(app['lat'] as number).toFixed(5)}, ${(app['lon'] as number).toFixed(5)}`]);
                        if (typeof app['freq'] === 'number') rows.push(['Frequency', `${((app['freq'] as number) / 1_000_000).toFixed(3)} MHz`]);
                        if (typeof app['sf'] === 'number') rows.push(['Spreading Factor', `SF${app['sf']}`]);
                        if (rows.length === 0) rows.push(['Payload', 'No decoded fields available']);
                        return rows.map(([k, v]) => (
                          <div key={k} className="feed-detail__adv-row">
                            <span className="feed-detail__info-label">{k}</span>
                            <span className="feed-detail__info-value">{v}</span>
                          </div>
                        ));
                      })()}
                    </div>
                  )}

                  {/* TextMessage sub-breakdown */}
                  {parsed.rawPayloadType === 2 && parsed.totalBytes - parsed.payloadStart >= 4 && (
                    <div className="feed-detail__breakdown" style={{ marginTop: '8px' }}>
                      <ByteSection
                        label="Destination Hash"
                        byteRange={byteRangeName(parsed.payloadStart, parsed.payloadStart + 1)}
                        hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.payloadStart, parsed.payloadStart + 1)}
                        description="First byte of destination node public key"
                      />
                      <ByteSection
                        label="Source Hash"
                        byteRange={byteRangeName(parsed.payloadStart + 1, parsed.payloadStart + 2)}
                        hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.payloadStart + 1, parsed.payloadStart + 2)}
                        description="First byte of source node public key"
                      />
                      {parsed.totalBytes - parsed.payloadStart >= 4 && (
                        <ByteSection
                          label="Cipher MAC"
                          byteRange={byteRangeName(parsed.payloadStart + 2, parsed.payloadStart + 4)}
                          hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.payloadStart + 2, parsed.payloadStart + 4)}
                          description="MAC for encrypted data"
                        />
                      )}
                      {parsed.totalBytes - parsed.payloadStart > 4 && (
                        <ByteSection
                          label="Ciphertext"
                          byteRange={byteRangeName(parsed.payloadStart + 4, parsed.totalBytes)}
                          hexValue={hexSlice(detail.rawHex.replace(/\s/g, ''), parsed.payloadStart + 4, parsed.totalBytes)}
                          description="Encrypted message data (timestamp + message text)"
                        />
                      )}
                    </div>
                  )}
                </ByteSection>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
};
