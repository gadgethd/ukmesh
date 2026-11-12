import assert from 'node:assert/strict';
import test from 'node:test';
import type { Response } from 'express';
import { createOwnerSessionGuard } from './ownerSessionGuard.js';

const SESSION = {
  v: 4 as const,
  mqttUsername: 'owner',
  sid: '11111111-1111-4111-8111-111111111111',
  exp: Date.now() + 60_000,
  gen: 7,
};

function responseRecorder() {
  const state: { statusCode: number; body: unknown; cleared: string[] } = {
    statusCode: 200,
    body: undefined,
    cleared: [],
  };
  const response = {
    clearCookie(name: string) {
      state.cleared.push(name);
      return response;
    },
    status(code: number) {
      state.statusCode = code;
      return response;
    },
    json(body: unknown) {
      state.body = body;
      return response;
    },
  } as unknown as Response;
  return { response, state };
}

function makeGuard(overrides: Partial<Parameters<typeof createOwnerSessionGuard>[0]> = {}) {
  return createOwnerSessionGuard({
    ownerCookieName: 'meshcore_owner_session',
    getOwnerSession: () => SESSION,
    getOwnerCredentialGeneration: async () => 7,
    isOwnerSessionActive: async () => true,
    touchOwnerSession: async () => undefined,
    resolveOwnerNodeIds: async () => ['A1'.repeat(32)],
    ...overrides,
  });
}

test('owner session guard allows an active session and touches it', async () => {
  let touched = '';
  const guard = makeGuard({ touchOwnerSession: async (sid) => { touched = sid; } });
  const { response, state } = responseRecorder();

  assert.deepEqual(await guard({} as never, response), ['A1'.repeat(32)]);
  assert.equal(touched, SESSION.sid);
  assert.equal(state.statusCode, 200);
});

test('owner session guard rejects a revoked session', async () => {
  const guard = makeGuard({ isOwnerSessionActive: async () => false });
  const { response, state } = responseRecorder();

  assert.equal(await guard({} as never, response), null);
  assert.equal(state.statusCode, 401);
  assert.deepEqual(state.body, { error: 'Session has been revoked — please log in again' });
});

test('owner session guard rejects a username-mismatched server row', async () => {
  let checked: [string, string] | undefined;
  const guard = makeGuard({
    isOwnerSessionActive: async (sid, username) => {
      checked = [sid, username];
      return false;
    },
  });
  const { response, state } = responseRecorder();

  assert.equal(await guard({} as never, response), null);
  assert.deepEqual(checked, [SESSION.sid, SESSION.mqttUsername]);
  assert.equal(state.statusCode, 401);
});

test('owner session guard fails closed when the session store is unavailable', async () => {
  const guard = makeGuard({
    isOwnerSessionActive: async () => { throw new Error('db offline'); },
  });
  const { response, state } = responseRecorder();

  assert.equal(await guard({} as never, response), null);
  assert.equal(state.statusCode, 503);
  assert.deepEqual(state.body, { error: 'Owner session store is temporarily unavailable' });
});
