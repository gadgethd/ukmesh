import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import * as maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { LoadingIndicator } from '../components/LoadingIndicator.js';
import { MAP_STYLE } from '../components/Map/mapConfig.js';
import './spam-page.css';
import { useWatchlist } from '../hooks/useWatchlist.js';

// ---------------------------------------------------------------------------
// Spam Watch — message-spam dashboard.
//
// Shows suspected spam clusters (repeated near-duplicate messages, rotating
// sender names) detected on the mesh. All data is pre-sanitized server-side:
// sender names are redacted, message samples are stripped of links/ids, and
// origins are coarse heat zones — never exact identities or locations.
// ---------------------------------------------------------------------------

type OriginLevel = 'high' | 'medium' | 'low' | 'insufficient';

interface PublicOrigin {
  region: string;
  confidence: number;
  level: OriginLevel;
  zone: { lat: number; lon: number; radiusKm: number } | null;
  observerCount: number;
  reasons: string[];
}

interface PublicIncident {
  id: string;
  status: 'active' | 'closed';
  network: string;
  firstSeen: string;
  lastSeen: string;
  messageCount: number;
  observerCount: number;
  channels: string[];
  similarUsernames: string[];
  usernameVariants: number;
  sampleMessage: string;
  spamMarker: boolean;
  confidence: number;
  reasons: string[];
  origin: PublicOrigin;
}

interface TimelineEntry {
  observedAt: string;
  channel: string;
  observerCount: number;
  minHopCount: number | null;
  bestRssi: number | null;
  bestSnr: number | null;
}

interface IncidentDetail extends PublicIncident {
  timeline: TimelineEntry[];
}

interface StatusResp {
  ongoing: boolean;
  activeIncidents: number;
  totalIncidents: number;
  messagesLast24h: number;
  observersInvolved: number;
  lastIncidentAt: string | null;
  updatedAt: string;
}

interface IncidentsResp {
  filters: { status: string; minConfidence: number; limit: number; offset: number };
  returned: number;
  incidents: PublicIncident[];
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function timeAgo(ts: string | null): string {
  if (!ts) return 'never';
  const diff = Math.max(0, Date.now() - Date.parse(ts));
  const sec = Math.floor(diff / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.floor(sec / 3600)}h ago`;
  return `${Math.floor(sec / 86400)}d ago`;
}

function fmtDateTime(ts: string): string {
  return new Date(ts).toLocaleString('en-GB', { dateStyle: 'medium', timeStyle: 'short' });
}

function confidenceClass(c: number): string {
  if (c >= 0.66) return 'sm-conf sm-conf--high';
  if (c >= 0.4) return 'sm-conf sm-conf--med';
  return 'sm-conf sm-conf--low';
}

function confidenceLabel(c: number): string {
  if (c >= 0.66) return 'High';
  if (c >= 0.4) return 'Medium';
  return 'Low';
}

function levelLabel(level: OriginLevel): string {
  switch (level) {
    case 'high': return 'High confidence';
    case 'medium': return 'Medium confidence';
    case 'low': return 'Low confidence';
    default: return 'Insufficient data';
  }
}

/** Build an approximate circle polygon (lon/lat ring) for a coarse heat zone. */
function circlePolygon(lat: number, lon: number, radiusKm: number, steps = 48): GeoJSON.Position[] {
  const ring: GeoJSON.Position[] = [];
  const dLat = radiusKm / 111.32;
  const dLon = radiusKm / (111.32 * Math.cos((lat * Math.PI) / 180) || 1);
  for (let i = 0; i <= steps; i++) {
    const theta = (i / steps) * 2 * Math.PI;
    ring.push([lon + dLon * Math.cos(theta), lat + dLat * Math.sin(theta)]);
  }
  return ring;
}

function buildZonesGeoJSON(incidents: PublicIncident[]): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: incidents
      .filter((i) => i.origin.zone)
      .map((i) => ({
        type: 'Feature' as const,
        properties: { id: i.id, confidence: i.confidence, active: i.status === 'active' },
        geometry: {
          type: 'Polygon' as const,
          coordinates: [circlePolygon(i.origin.zone!.lat, i.origin.zone!.lon, i.origin.zone!.radiusKm)],
        },
      })),
  };
}

const EMPTY_FC: GeoJSON.FeatureCollection = { type: 'FeatureCollection', features: [] };

// On-brand zone colours (danger for active, amber for closed) — see design tokens.
const ZONE_ACTIVE = '#ff3860';
const ZONE_CLOSED = '#ffb300';

// ---------------------------------------------------------------------------
// Per-incident origin map — the coarse confidence circle for one incident.
// Lazily mounted (only when the user opens it) and fitted to the zone.
// ---------------------------------------------------------------------------

const OriginMiniMap = memo(function OriginMiniMap({
  zone,
  active,
}: {
  zone: { lat: number; lon: number; radiusKm: number };
  active: boolean;
}) {
  const el = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);

  useEffect(() => {
    if (!el.current || mapRef.current) return;
    const ring = circlePolygon(zone.lat, zone.lon, zone.radiusKm);
    const lons = ring.map((p) => p[0]);
    const lats = ring.map((p) => p[1]);
    const bounds: maplibregl.LngLatBoundsLike = [
      [Math.min(...lons), Math.min(...lats)],
      [Math.max(...lons), Math.max(...lats)],
    ];
    const color = active ? ZONE_ACTIVE : ZONE_CLOSED;

    const map = new maplibregl.Map({
      container: el.current,
      style: MAP_STYLE,
      bounds,
      fitBoundsOptions: { padding: 40, maxZoom: 10 },
      attributionControl: false,
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');
    map.on('load', () => {
      map.addSource('zone', {
        type: 'geojson',
        data: {
          type: 'Feature',
          properties: {},
          geometry: { type: 'Polygon', coordinates: [ring] },
        },
      });
      map.addLayer({ id: 'zone-fill', type: 'fill', source: 'zone', paint: { 'fill-color': color, 'fill-opacity': 0.2 } });
      map.addLayer({
        id: 'zone-outline',
        type: 'line',
        source: 'zone',
        paint: { 'line-color': color, 'line-width': 1.5, 'line-opacity': 0.85 },
      });
      map.addSource('center', {
        type: 'geojson',
        data: { type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: [zone.lon, zone.lat] } },
      });
      map.addLayer({
        id: 'zone-center',
        type: 'circle',
        source: 'center',
        paint: { 'circle-radius': 4, 'circle-color': color, 'circle-stroke-color': '#0a1628', 'circle-stroke-width': 1.5 },
      });
    });
    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, [zone.lat, zone.lon, zone.radiusKm, active]);

  return (
    <div className="sm-origin-map">
      <div ref={el} className="sm-origin-map__canvas" />
      <div className="sm-origin-map__note">
        Shaded area ≈ {zone.radiusKm} km confidence radius — broad estimate, not a precise location.
      </div>
    </div>
  );
});

// ---------------------------------------------------------------------------
// Incident card
// ---------------------------------------------------------------------------

const IncidentCard = memo(function IncidentCard({
  incident,
  mapOpen,
  onToggleMap,
}: {
  incident: PublicIncident;
  mapOpen: boolean;
  onToggleMap: () => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const [detail, setDetail] = useState<IncidentDetail | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const detailController = useRef<AbortController | null>(null);
  const watchlist = useWatchlist();

  useEffect(() => () => detailController.current?.abort(), []);

  const toggle = useCallback(() => {
    const next = !expanded;
    setExpanded(next);
    if (next && !detail && !loadingDetail) {
      detailController.current?.abort();
      const controller = new AbortController();
      detailController.current = controller;
      setLoadingDetail(true);
      fetch(`/api/spam/messages/incidents/${incident.id}`, {
        cache: 'no-store',
        signal: controller.signal,
      })
        .then((r) => (r.ok ? r.json() : Promise.reject(new Error(String(r.status)))))
        .then((d: IncidentDetail) => {
          if (!controller.signal.aborted) setDetail(d);
        })
        .catch(() => {
          if (!controller.signal.aborted) setDetail(null);
        })
        .finally(() => {
          if (!controller.signal.aborted) setLoadingDetail(false);
        });
    }
  }, [expanded, detail, loadingDetail, incident.id]);

  const o = incident.origin;

  return (
    <div
      id={`spam-incident-${incident.id}`}
      className={`sm-card ${incident.status === 'active' ? 'sm-card--active' : ''}`}
      tabIndex={-1}
    >
      <div className="sm-card__head">
        <div className="sm-card__title">
          <span className="sm-card__label">Suspected spam cluster</span>
          {incident.spamMarker && <span className="sm-tag sm-tag--marker">marker</span>}
          {incident.status === 'active' && <span className="sm-tag sm-tag--active">ongoing</span>}
        </div>
        <span className={confidenceClass(incident.confidence)} title="Detection confidence">
          {confidenceLabel(incident.confidence)} · {Math.round(incident.confidence * 100)}%
        </span>
        <button type="button" className="sm-watch" onClick={() => watchlist.toggle('spam_incident', incident.id, `Spam incident ${incident.id.slice(0, 8)}`)}>
          {watchlist.isWatched('spam_incident', incident.id) ? '★ Watching' : '☆ Watch'}
        </button>
      </div>

      <blockquote className="sm-sample">“{incident.sampleMessage}”</blockquote>

      <div className="sm-grid">
        <div><span className="sm-k">Messages</span><span className="sm-v">{incident.messageCount}</span></div>
        <div><span className="sm-k">Observers</span><span className="sm-v">{incident.observerCount}</span></div>
        <div><span className="sm-k">First seen</span><span className="sm-v" title={fmtDateTime(incident.firstSeen)}>{timeAgo(incident.firstSeen)}</span></div>
        <div><span className="sm-k">Last seen</span><span className="sm-v" title={fmtDateTime(incident.lastSeen)}>{timeAgo(incident.lastSeen)}</span></div>
        <div><span className="sm-k">Channels</span><span className="sm-v">{incident.channels.join(', ') || '—'}</span></div>
        <div><span className="sm-k">Name variants</span><span className="sm-v">{incident.usernameVariants}</span></div>
      </div>

      <div className="sm-origin">
        <button
          type="button"
          className="sm-origin__head sm-origin__toggle"
          onClick={() => o.zone && onToggleMap()}
          disabled={!o.zone}
          aria-expanded={mapOpen}
          title={o.zone ? 'Show the coarse origin area on a map' : 'No location estimate available'}
        >
          <span className="sm-k">Estimated origin</span>
          <span className={`sm-level sm-level--${o.level}`}>{levelLabel(o.level)}</span>
        </button>
        <div className="sm-origin__body">
          <strong>{o.region}</strong>
          {o.zone && <span className="sm-origin__radius"> · ≈ {o.zone.radiusKm} km area</span>}
          {o.level !== 'insufficient' && <span className="sm-origin__conf"> · {Math.round(o.confidence * 100)}% confidence</span>}
        </div>
        {o.reasons.length > 0 && <div className="sm-reasons">{o.reasons.join(' · ')}</div>}
        {o.zone && (
          <button type="button" className="sm-origin__maplink" onClick={onToggleMap}>
            {mapOpen ? '▾ Hide map' : '🗺 View area on map'}
          </button>
        )}
        {mapOpen && o.zone && <OriginMiniMap zone={o.zone} active={incident.status === 'active'} />}
      </div>

      {incident.similarUsernames.length > 0 && (
        <div className="sm-usernames">
          <span className="sm-k">Similar sender names</span>
          <div className="sm-chips">
            {incident.similarUsernames.map((u, idx) => <span key={idx} className="sm-chip">{u}</span>)}
          </div>
        </div>
      )}

      <button
        type="button"
        className="sm-expand"
        onClick={toggle}
        aria-expanded={expanded}
        aria-controls={`spam-incident-detail-${incident.id}`}
      >
        {expanded ? '▾ Hide details' : '▸ Why flagged & timeline'}
      </button>

      {expanded && (
        <div className="sm-detail" id={`spam-incident-detail-${incident.id}`}>
          <div className="sm-detail__reasons">
            <span className="sm-k">Detection factors</span>
            <ul>{incident.reasons.map((r, idx) => <li key={idx}>{r}</li>)}</ul>
          </div>
          {loadingDetail && <div className="sm-muted">Loading timeline…</div>}
          {detail && detail.timeline.length > 0 && (
            <div className="sm-timeline">
              <span className="sm-k">Timeline ({detail.timeline.length} transmissions)</span>
              <table>
                <thead>
                  <tr><th scope="col">Time</th><th scope="col">Channel</th><th scope="col">Observers</th><th scope="col">Min hops</th><th scope="col">Best SNR</th></tr>
                </thead>
                <tbody>
                  {detail.timeline.slice(0, 40).map((t, idx) => (
                    <tr key={idx}>
                      <td title={fmtDateTime(t.observedAt)}>{fmtDateTime(t.observedAt)}</td>
                      <td>{t.channel}</td>
                      <td>{t.observerCount}</td>
                      <td>{t.minHopCount ?? '—'}</td>
                      <td>{t.bestSnr != null ? `${t.bestSnr} dB` : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
});

// The API caps this list at 200 incidents. Keeping expanded cards in normal
// document flow avoids the fixed-row virtualization overlap bug.
function IncidentList({
  incidents,
  openMapId,
  setOpenMapId,
}: {
  incidents: PublicIncident[];
  openMapId: string | null;
  setOpenMapId: (id: string | null) => void;
}) {
  return (
    <div className="sm-incident-list">
      {incidents.map((incident) => (
        <IncidentCard
          key={incident.id}
          incident={incident}
          mapOpen={openMapId === incident.id}
          onToggleMap={() => setOpenMapId(openMapId === incident.id ? null : incident.id)}
        />
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export function SpamPage() {
  const [status, setStatus] = useState<StatusResp | null>(null);
  const [incidents, setIncidents] = useState<PublicIncident[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showLow, setShowLow] = useState(false);
  const [openMapId, setOpenMapId] = useState<string | null>(null);
  const [retryVersion, setRetryVersion] = useState(0);
  const requestedIncident = new URLSearchParams(window.location.search).get('incident')?.trim() ?? '';

  const mapEl = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const mapReady = useRef(false);
  const zonesRef = useRef<PublicIncident[]>([]);

  useEffect(() => {
    if (!requestedIncident || !incidents?.some((incident) => incident.id === requestedIncident)) return;
    const frame = window.requestAnimationFrame(() => {
      const card = document.getElementById(`spam-incident-${requestedIncident}`);
      card?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      card?.focus({ preventScroll: true });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [incidents, requestedIncident]);

  // Fetch status + incidents whenever the confidence filter changes.
  useEffect(() => {
    const controller = new AbortController();
    setError(null);
    setIncidents(null);
    const conf = showLow ? '?minConfidence=0' : '';
    const incConf = showLow ? '&minConfidence=0' : '';
    Promise.all([
      fetch(`/api/spam/messages/status${conf}`, {
        cache: 'no-store',
        signal: controller.signal,
      }).then((response) => {
        if (!response.ok) throw new Error(`status:${response.status}`);
        return response.json() as Promise<StatusResp>;
      }),
      fetch(`/api/spam/messages/incidents?limit=200${incConf}`, {
        cache: 'no-store',
        signal: controller.signal,
      }).then(
        (response) => {
          if (!response.ok) throw new Error(`incidents:${response.status}`);
          return response.json() as Promise<IncidentsResp>;
        },
      ),
    ])
      .then(([s, inc]) => {
        if (controller.signal.aborted) return;
        setStatus(s);
        setIncidents(Array.isArray(inc.incidents) ? inc.incidents : []);
      })
      .catch(() => {
        if (!controller.signal.aborted) {
          setStatus(null);
          setIncidents([]);
          setError('Could not load spam data. Please try again later.');
        }
      });
    return () => controller.abort();
  }, [retryVersion, showLow]);

  const active = useMemo(() => (incidents ?? []).filter((i) => i.status === 'active'), [incidents]);
  const historical = useMemo(() => (incidents ?? []).filter((i) => i.status === 'closed'), [incidents]);
  const zones = useMemo(() => (incidents ?? []).filter((i) => i.origin.zone), [incidents]);
  zonesRef.current = zones;

  // Initialise the coarse heat-zone map once.
  useEffect(() => {
    if (!mapEl.current || mapRef.current) return;
    const map = new maplibregl.Map({
      container: mapEl.current,
      style: MAP_STYLE,
      center: [-2.5, 54.0],
      zoom: 4.6,
      attributionControl: false,
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');
    map.on('load', () => {
      map.addSource('zones', { type: 'geojson', data: EMPTY_FC });
      map.addLayer({
        id: 'zones-fill',
        type: 'fill',
        source: 'zones',
        paint: {
          'fill-color': ['case', ['get', 'active'], ZONE_ACTIVE, ZONE_CLOSED],
          'fill-opacity': 0.16,
        },
      });
      map.addLayer({
        id: 'zones-outline',
        type: 'line',
        source: 'zones',
        paint: {
          'line-color': ['case', ['get', 'active'], ZONE_ACTIVE, ZONE_CLOSED],
          'line-width': 1.5,
          'line-opacity': 0.75,
        },
      });
      mapReady.current = true;
      const src = map.getSource('zones') as maplibregl.GeoJSONSource | undefined;
      src?.setData(buildZonesGeoJSON(zonesRef.current));
    });
    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
      mapReady.current = false;
    };
  }, []);

  // Push zone updates to the map.
  useEffect(() => {
    if (!mapRef.current || !mapReady.current) return;
    const src = mapRef.current.getSource('zones') as maplibregl.GeoJSONSource | undefined;
    src?.setData(buildZonesGeoJSON(zones));
  }, [zones]);

  return (
    <div className="sm-page">
      <header className="sm-header">
        <span className="sm-kicker">Network Integrity · Abuse Mitigation</span>
        <h1>Spam Watch</h1>
        <p className="sm-sub">
          Suspected message-spam on the UK mesh — repeated near-duplicate messages and rotating sender
          names, grouped into incidents. All output is sanitized: no exact identities or locations are shown.
          Clusters are <em>suspected</em> patterns, not accusations.
        </p>
      </header>

      {error && (
        <div className="sm-error" role="alert">
          {error}{' '}
          <button type="button" onClick={() => setRetryVersion((value) => value + 1)}>
            Retry
          </button>
        </div>
      )}

      {!incidents && !error && <LoadingIndicator />}

      {status && (
        <div className={`sm-status ${status.ongoing ? 'sm-status--alert' : 'sm-status--clear'}`}>
          <div className="sm-status__main">
            <span className="sm-status__dot" />
            <span className="sm-status__text">
              {status.ongoing
                ? `${status.activeIncidents} spam incident${status.activeIncidents === 1 ? '' : 's'} ongoing right now`
                : 'No ongoing spam detected'}
            </span>
          </div>
          <div className="sm-status__meta">
            <span>{status.totalIncidents} tracked incident{status.totalIncidents === 1 ? '' : 's'}</span>
            <span>·</span>
            <span>last activity {timeAgo(status.lastIncidentAt)}</span>
          </div>
        </div>
      )}

      <div className="sm-controls">
        <label className="sm-toggle">
          <input type="checkbox" checked={showLow} onChange={(e) => setShowLow(e.target.checked)} />
          Show lower-confidence clusters
        </label>
      </div>

      <section className="sm-mapwrap" aria-label="Coarse incident origin map">
        <div className="sm-maphint">
          {incidents === null
            ? 'Loading coarse origin heat zones…'
            : error
              ? 'Origin zones are unavailable.'
              : zones.length === 0
                ? 'No coarse origin zones are available for this result.'
                : 'Coarse origin heat zones — broad areas only, not precise locations.'}
        </div>
        <div ref={mapEl} className="sm-map" />
      </section>

      {incidents && (
        <>
          <section className="sm-section">
            <h2>Ongoing incidents {active.length > 0 && <span className="sm-count">{active.length}</span>}</h2>
            {active.length === 0 ? (
              <p className="sm-muted">Nothing active. The mesh looks clean right now.</p>
            ) : (
              <IncidentList incidents={active} openMapId={openMapId} setOpenMapId={setOpenMapId} />
            )}
          </section>

          <section className="sm-section">
            <h2>Historical incidents {historical.length > 0 && <span className="sm-count">{historical.length}</span>}</h2>
            {historical.length === 0 ? (
              <p className="sm-muted">No past incidents recorded{showLow ? '' : ' above the confidence threshold'}.</p>
            ) : (
              <IncidentList incidents={historical} openMapId={openMapId} setOpenMapId={setOpenMapId} />
            )}
          </section>
        </>
      )}

      <footer className="sm-footer">
        How to read this: confidence reflects how strongly the pattern resembles spam (volume, repeated links,
        rotating names). Origin estimates combine many observers' signal strength and hop counts into a broad
        area — more observers means a tighter estimate. “Insufficient data” means too few observers to locate.
      </footer>
    </div>
  );
}
