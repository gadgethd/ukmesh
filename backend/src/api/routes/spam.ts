import { Router, RequestHandler } from 'express';
import { getSpamSuspects, getSpamPacketObservers, getSpamAllObservers, getSpamSuspectSummary } from '../../db/index.js';
import { getPublicIncident, getPublicIncidents, getPublicStatus } from '../../spam/repository.js';
import { loadSpamMessageConfig } from '../../spam/config.js';
import {
  parseBoolean,
  parseBoundedFloat,
  parseBoundedInteger,
  parseEnum,
} from '../utils/input.js';
import { API_ERROR_CODES, sendApiError } from '../errors.js';

interface SpamRouteDeps {
  expensiveLimiter: RequestHandler;
}

const SPAM_MSG_CFG = loadSpamMessageConfig();

function numberParam(value: unknown, fallback: number, min: number, max: number): number {
  return parseBoundedInteger(value, {
    name: 'numeric parameter',
    defaultValue: fallback,
    min,
    max,
  });
}

function floatParam(value: unknown, fallback: number, min: number, max: number): number {
  return parseBoundedFloat(value, {
    name: 'numeric parameter',
    defaultValue: fallback,
    min,
    max,
  });
}

function boolParam(value: unknown): boolean {
  return parseBoolean(value, { name: 'boolean parameter' });
}

function verdictParam(value: unknown): 'spam' | 'suspect' | undefined {
  return parseEnum(value, {
    name: 'verdict',
    values: ['spam', 'suspect'] as const,
  });
}

function statusParam(value: unknown): 'active' | 'closed' | undefined {
  return parseEnum(value, {
    name: 'status',
    values: ['active', 'closed'] as const,
  });
}

export function registerSpamRoutes(router: Router, deps: SpamRouteDeps): void {
  const { expensiveLimiter } = deps;

  // -------------------------------------------------------------------------
  // Message-spam dashboard (public, sanitized).
  // -------------------------------------------------------------------------

  // Current spam status: is anything ongoing right now?
  router.get('/spam/messages/status', async (req, res) => {
    const minConfidence = floatParam(req.query['minConfidence'], SPAM_MSG_CFG.publicMinScore, 0, 1);
    try {
      res.json(await getPublicStatus(minConfidence));
    } catch (err: unknown) {
      console.error('[api/spam] message status error:', (err as Error).message);
      res.status(500).json({ error: 'internal error' });
    }
  });

  // List incidents (active and/or historical), already sanitized.
  router.get('/spam/messages/incidents', expensiveLimiter, async (req, res) => {
    const status = statusParam(req.query['status']);
    const limit = numberParam(req.query['limit'], 100, 1, 200);
    const offset = numberParam(req.query['offset'], 0, 0, 100000);
    const minConfidence = floatParam(req.query['minConfidence'], SPAM_MSG_CFG.publicMinScore, 0, 1);
    try {
      const incidents = await getPublicIncidents({ status, limit, offset, minConfidence });
      res.json({
        filters: { status: status ?? 'all', minConfidence, limit, offset },
        returned: incidents.length,
        incidents,
      });
    } catch (err: unknown) {
      console.error('[api/spam] message incidents error:', (err as Error).message);
      res.status(500).json({ error: 'internal error' });
    }
  });

  // Single incident with sanitized timeline.
  router.get('/spam/messages/incidents/:id', expensiveLimiter, async (req, res) => {
    const rawId = req.params.id;
    const id = Array.isArray(rawId) ? rawId[0] : rawId;
    if (!id || !/^[0-9a-f]{16}$/.test(id)) {
      sendApiError(res, 400, 'invalid incident id', API_ERROR_CODES.invalidIncidentId);
      return;
    }
    try {
      const incident = await getPublicIncident(id);
      if (!incident) {
        res.status(404).json({ error: 'not found' });
        return;
      }
      res.json(incident);
    } catch (err: unknown) {
      console.error('[api/spam] message incident error:', (err as Error).message);
      res.status(500).json({ error: 'internal error' });
    }
  });

  // -------------------------------------------------------------------------
  // Legacy advert/identity-spoof suspect endpoints (kept for the MQTT-side
  // detection data; not used by the message-spam dashboard).
  // -------------------------------------------------------------------------

  router.get('/spam/suspects', expensiveLimiter, async (req, res) => {
    const hours = numberParam(req.query['hours'], 8760, 1, 8760);
    const includeSuspects = boolParam(req.query['includeSuspects']);
    const requestedVerdict = verdictParam(req.query['verdict']);
    const verdict = requestedVerdict ?? (includeSuspects ? undefined : 'spam');
    const minScore = req.query['minScore'] == null
      ? undefined
      : numberParam(req.query['minScore'], 0, 0, 1000);
    const limit = numberParam(req.query['limit'], 100, 1, 200);
    const offset = numberParam(req.query['offset'], 0, 0, 100000);
    const includePacketCounts = boolParam(req.query['includePacketCounts']);
    try {
      const filter = { hours, verdict, minScore };
      const [rows, summary, filteredSummary] = await Promise.all([
        getSpamSuspects({ ...filter, limit, offset, includePacketCounts }),
        getSpamSuspectSummary({ hours }),
        getSpamSuspectSummary(filter),
      ]);

      const byName = new Map<string, {
        spoofed_name: string;
        verdict: string;
        max_score: number;
        canonical_key: string | null;
        suspects: typeof rows;
      }>();

      for (const row of rows) {
        const name = row.spoofed_name as string;
        let group = byName.get(name);
        if (!group) {
          group = {
            spoofed_name:  name,
            verdict:       row.verdict as string,
            max_score:     Number(row.total_score),
            canonical_key: row.canonical_key as string | null,
            suspects:      [],
          };
          byName.set(name, group);
        }
        if (Number(row.total_score) > group.max_score) {
          group.max_score = Number(row.total_score);
          group.verdict   = row.verdict as string;
        }
        group.suspects.push(row);
      }

      res.json({
        total:           filteredSummary.total,
        returned:        rows.length,
        limit,
        offset,
        filters:         { hours, verdict: verdict ?? 'all', minScore: minScore ?? null, includePacketCounts },
        summary,
        filteredSummary,
        groups:          Array.from(byName.values()).sort((a, b) => b.max_score - a.max_score),
      });
    } catch (err: unknown) {
      console.error('[api/spam] suspects error:', (err as Error).message);
      res.status(500).json({ error: 'internal error' });
    }
  });

  // Bulk observer data for all spam suspects — used to draw all propagation lines on the map
  router.get('/spam/observers', expensiveLimiter, async (_req, res) => {
    try {
      const rows = await getSpamAllObservers();

      const byId = new Map<string, {
        src_node_id: string;
        claimed_lat: number;
        claimed_lon: number;
        spoofed_name: string;
        observers: Array<{
          observer_id: string;
          observer_name: string | null;
          observer_lat: number;
          observer_lon: number;
          hop_count: number | null;
          rssi: number | null;
        }>;
      }>();

      for (const row of rows) {
        const id = row.src_node_id as string;
        if (!byId.has(id)) {
          byId.set(id, {
            src_node_id:  id,
            claimed_lat:  Number(row.claimed_lat),
            claimed_lon:  Number(row.claimed_lon),
            spoofed_name: row.spoofed_name as string,
            observers:    [],
          });
        }
        if (row.observer_id) {
          byId.get(id)!.observers.push({
            observer_id:   row.observer_id as string,
            observer_name: row.observer_name as string | null,
            observer_lat:  Number(row.observer_lat),
            observer_lon:  Number(row.observer_lon),
            hop_count:     row.hop_count != null ? Number(row.hop_count) : null,
            rssi:          row.rssi != null ? Number(row.rssi) : null,
          });
        }
      }

      res.json(Array.from(byId.values()));
    } catch (err: unknown) {
      console.error('[api/spam] observers error:', (err as Error).message);
      res.status(500).json({ error: 'internal error' });
    }
  });

  // Per-node observer detail (used as fallback)
  router.get('/spam/packet/:id/observers', expensiveLimiter, async (req, res) => {
    const rawId = req.params.id;
    const id = Array.isArray(rawId) ? rawId[0] : rawId;
    if (!id || !/^[0-9a-fA-F]{64}$/.test(id)) {
      sendApiError(res, 400, 'invalid node id', API_ERROR_CODES.invalidNodeId);
      return;
    }
    try {
      const rows = await getSpamPacketObservers(id);
      const first = rows[0];
      res.json({
        claimed_lat:  first?.claimed_lat ?? null,
        claimed_lon:  first?.claimed_lon ?? null,
        spoofed_name: first?.spoofed_name ?? null,
        observers:    rows.map((r) => ({
          observer_id:   r.node_id,
          observer_name: r.name,
          observer_lat:  r.lat,
          observer_lon:  r.lon,
          iata:          r.iata,
          hop_count:     r.hop_count,
          rssi:          r.rssi,
          time:          r.time,
        })),
      });
    } catch (err: unknown) {
      console.error('[api/spam] observers error:', (err as Error).message);
      res.status(500).json({ error: 'internal error' });
    }
  });
}
