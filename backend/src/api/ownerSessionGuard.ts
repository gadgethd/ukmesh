import type { Request, Response } from 'express';
import type { OwnerSession } from '../owner/ownerSession.js';

export type OwnerSessionGuardDeps = {
  ownerCookieName: string;
  getOwnerSession: (req: Request) => OwnerSession | null;
  getOwnerCredentialGeneration: (mqttUsername: string) => Promise<number>;
  isOwnerSessionActive: (sessionId: string, mqttUsername: string) => Promise<boolean>;
  touchOwnerSession: (sessionId: string) => Promise<void>;
  resolveOwnerNodeIds: (mqttUsername: string) => Promise<string[]>;
};

export function createOwnerSessionGuard(deps: OwnerSessionGuardDeps) {
  return async function requireOwnerSession(req: Request, res: Response): Promise<string[] | null> {
    const session = deps.getOwnerSession(req);
    if (!session) {
      res.clearCookie(deps.ownerCookieName, { path: '/' });
      res.status(401).json({ error: 'Not logged in' });
      return null;
    }
    try {
      const currentGen = await deps.getOwnerCredentialGeneration(session.mqttUsername);
      if (session.gen !== currentGen) {
        res.clearCookie(deps.ownerCookieName, { path: '/' });
        res.status(401).json({ error: 'Credentials have been rotated — please log in again' });
        return null;
      }
      const active = await deps.isOwnerSessionActive(session.sid, session.mqttUsername);
      if (!active) {
        res.clearCookie(deps.ownerCookieName, { path: '/' });
        res.status(401).json({ error: 'Session has been revoked — please log in again' });
        return null;
      }
      void deps.touchOwnerSession(session.sid).catch((error) => {
        console.warn('[owner-session] touch failed', error instanceof Error ? error.message : error);
      });
      const nodeIds = await deps.resolveOwnerNodeIds(session.mqttUsername);
      if (nodeIds.length < 1) {
        res.clearCookie(deps.ownerCookieName, { path: '/' });
        res.status(401).json({ error: 'Owner authorization has been revoked' });
        return null;
      }
      return nodeIds;
    } catch (error) {
      console.error('[api] owner session validation failed', error instanceof Error ? error.message : error);
      res.status(503).json({ error: 'Owner session store is temporarily unavailable' });
      return null;
    }
  };
}
