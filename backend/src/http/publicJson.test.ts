import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import test from 'node:test';
import express from 'express';
import { PreparedJson, publicJson, PUBLIC_REVALIDATE } from './publicJson.js';

test('conditional public reads fence cached bodies, each scope and in-flight changes', async (t) => {
  const app = express();
  let generation = 1;
  let unavailable = false;
  let changeDuringLoad = false;
  let failFinalRead = false;
  let reads = 0;
  let loads = 0;
  app.get('/', async (req, res) => {
    reads = 0;
    try {
      await publicJson(res, async () => {
        reads++;
        if (unavailable || (failFinalRead && reads === 2)) throw new Error('unavailable');
        return generation;
      }, async () => {
        loads++;
        if (changeDuringLoad) generation++;
        return { network: req.query.network, observer: req.query.observer,
          region: req.query.region, sections: req.query.sections, redacted: generation > 1 };
      });
    } catch {
      res.status(503).json({ error: 'Unavailable' });
    }
  });
  const server = app.listen(0, '127.0.0.1');
  t.after(() => { server.closeAllConnections(); server.close(); });
  await once(server, 'listening');
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const first = await fetch(`${base}/?network=ukmesh`);
  assert.equal(first.status, 200);
  assert.equal(first.headers.get('cache-control'), PUBLIC_REVALIDATE);
  const etag = first.headers.get('etag')!;
  assert.ok(etag, 'use the existing Express validator');
  // Undici adds request Cache-Control: no-cache for conditional default-cache
  // requests; force-cache lets the test exercise native HTTP validation.
  const conditional = () => ({ cache: 'force-cache' as const, headers: { 'If-None-Match': etag } });
  const unchanged = await fetch(`${base}/?network=ukmesh`, conditional());
  assert.equal(unchanged.status, 304);
  assert.equal(reads, 2);
  assert.equal(loads, 2, '304 still validates current origin authority and payload');
  for (const query of ['network=test', 'network=ukmesh&observer=fixture',
    'network=ukmesh&region=fixture', 'network=ukmesh&sections=overview']) {
    assert.equal((await fetch(`${base}/?${query}`, conditional())).status, 200);
  }
  generation++;
  const redacted = await fetch(`${base}/?network=ukmesh`, conditional());
  assert.equal(redacted.status, 200);
  assert.equal((await redacted.json()).redacted, true);
  for (const mode of ['initial failure', 'final failure', 'in-flight change']) {
    unavailable = mode === 'initial failure';
    failFinalRead = mode === 'final failure';
    changeDuringLoad = mode === 'in-flight change';
    const result = await fetch(`${base}/?network=ukmesh`, conditional());
    assert.equal(result.status, 503, mode);
    assert.equal(result.headers.get('cache-control'), 'no-store', mode);
    assert.deepEqual(await result.json(), { error: 'Unavailable' });
  }
});

async function start(t: test.TestContext, app: express.Express) {
  const server = app.listen(0, '127.0.0.1');
  t.after(async () => {
    server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
  });
  await once(server, 'listening');
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

test('prepared delivery matches Express JSON bytes, content type and conditional ETag', async t => {
  const app = express();
  const value = { nodes: [{ node_id: 'A', name: '🛰️ "node" \\ \n<>&\u2028', lat: 54.5, firmware: null }], page: { complete: true } };
  const body = JSON.stringify(value);
  let reads = 0, loads = 0, clones = 0;
  const shared = PreparedJson(value, body, () => { clones++; throw new Error('default delivery must not clone'); });
  app.use((req, res, next) => {
    if (req.query.preset) res.type('application/vnd.ukmesh.map+json');
    next();
  });
  app.get('/express', (_req, res) => { res.json(value); });
  app.get('/prepared', async (_req, res) => {
    reads = 0;
    await publicJson(res, async () => { reads++; return 1; }, async () => {
      loads++; return shared;
    });
  });
  const base = await start(t, app);
  for (const query of ['', '?preset=1']) {
    const control = await fetch(base + '/express' + query), prepared = await fetch(base + '/prepared' + query);
    assert.equal(await prepared.text(), await control.text(), 'same exact encoded HTTP body');
    for (const header of ['content-type', 'content-length', 'etag']) {
      assert.equal(prepared.headers.get(header), control.headers.get(header), header);
    }
    assert.equal(reads, 2);
    assert.equal(prepared.headers.get('cache-control'), PUBLIC_REVALIDATE);
    const before = loads;
    const cached = await fetch(base + '/prepared' + query, {
      cache: 'force-cache', headers: { 'If-None-Match': control.headers.get('etag')! },
    });
    assert.equal(cached.status, 304);
    assert.equal(await cached.text(), '');
    assert.equal(reads, 2, 'conditional validation happens after both authority checks');
    assert.equal(loads, before + 1);
    const controlHead = await fetch(base + '/express' + query, { method: 'HEAD' });
    const preparedHead = await fetch(base + '/prepared' + query, { method: 'HEAD' });
    assert.equal(await preparedHead.text(), '');
    for (const header of ['content-type', 'content-length', 'etag']) {
      assert.equal(preparedHead.headers.get(header), controlHead.headers.get(header), `HEAD/${header}`);
    }
    assert.equal(reads, 2);
    assert.equal(clones, 0, 'shared default delivery, including HEAD/304, needs no value copy');
  }
});

test('prepared markers expose no structured value, encoded body or clone callback', () => {
  const marker = PreparedJson({ nested: { private: true } }, '{"nested":{"private":true}}', structuredClone) as object;
  assert.equal(Object.isFrozen(marker), true);
  assert.deepEqual(Reflect.ownKeys(marker), []);
  assert.equal('value' in marker, false);
  assert.equal('encodedBody' in marker, false);
  assert.equal('cloneForDelivery' in marker, false);
  assert.equal(JSON.stringify(marker), '{}');
});

test('prepared default delivery does not serialize the original value again', async t => {
  let serializations = 0;
  const value = { toJSON() { serializations++; return { visible: 'same representation' }; } };
  const encoded = JSON.stringify(value);
  const app = express();
  app.get('/', async (_req, res) => {
    await publicJson(res, async () => 1, async () => PreparedJson(value, encoded));
  });
  const base = await start(t, app);
  assert.equal(await (await fetch(base)).text(), encoded);
  assert.equal(serializations, 1);
});

test('custom JSON formatting delegates prepared values to the original Express serializer', async t => {
  const value = { visible: '<node>&🛰️', secret: 'remove me', nested: { count: 2 } };
  const settings: Array<[string, unknown]> = [
    ['json replacer', (key: string, item: unknown) => key === 'secret' ? undefined : item],
    ['json spaces', 2],
    ['json escape', true],
  ];
  for (const [setting, configured] of settings) {
    const app = express(); app.set(setting, configured);
    app.get('/express', (_req, res) => { res.json(value); });
    app.get('/prepared', async (_req, res) => {
      await publicJson(res, async () => 1, async () => PreparedJson(value, JSON.stringify(value)));
    });
    const base = await start(t, app);
    const control = await fetch(base + '/express'), prepared = await fetch(base + '/prepared');
    assert.equal(await prepared.text(), await control.text(), setting);
    for (const header of ['content-type', 'content-length', 'etag']) {
      assert.equal(prepared.headers.get(header), control.headers.get(header), `${setting}/${header}`);
    }
  }
});

test('prepared bodies stay unwritten while final authority is pending and fail closed', async t => {
  for (const outcome of ['changed', 'failed']) {
    let release!: () => void, reached!: () => void, reads = 0, clones = 0;
    const waiting = new Promise<void>(resolve => { reached = resolve; });
    const gate = new Promise<void>(resolve => { release = resolve; });
    t.after(() => release());
    let heldResponse: express.Response | undefined;
    const app = express();
    app.set('json spaces', 2);
    app.get('/', async (_req, res) => {
      heldResponse = res;
      try {
        await publicJson(res, async () => {
          if (++reads === 1) return 1;
          reached(); await gate;
          if (outcome === 'failed') throw new Error('authority unavailable');
          return 2;
        }, async () => PreparedJson({ secret: 'must not be sent' }, '{"secret":"must not be sent"}', value => {
          clones++;
          return structuredClone(value);
        }));
      } catch { res.status(503).json({ error: 'Unavailable' }); }
    });
    const base = await start(t, app);
    const pending = fetch(base);
    await waiting;
    assert.equal(heldResponse?.headersSent, false, outcome);
    assert.equal(heldResponse?.writableEnded, false, outcome);
    assert.equal(clones, 0, 'custom delivery cannot clone before final authority succeeds');
    release();
    const response = await pending;
    assert.equal(response.status, 503);
    assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.deepEqual(await response.json(), { error: 'Unavailable' });
    assert.equal(clones, 0, 'changed/unavailable authority refuses before exposing a delivery copy');
  }
});

test('each custom serializer gets its own nested PostgreSQL value and Date copy', async t => {
  const value = { rows: [{ at: new Date('2026-01-01T00:00:00Z'), payload: { count: 2 } }], text: '<>&' };
  const encoded = JSON.stringify(value);
  const copies: typeof value[] = [];
  const marker = PreparedJson(value, encoded, original => {
    const copy = structuredClone(original);
    assert.ok(copy.rows[0]!.at instanceof Date);
    copies.push(copy);
    return copy;
  });
  const app = express();
  app.set('json replacer', (key: string, item: unknown) => {
    if (key === '') {
      const root = item as typeof value;
      root.rows[0]!.payload.count++;
      root.rows[0]!.at.setUTCFullYear(2030);
    }
    return item;
  });
  const authorityCounts: number[] = [];
  app.get('/', async (_req, res) => {
    let reads = 0;
    await publicJson(res, async () => { reads++; return 1; }, async () => marker);
    authorityCounts.push(reads);
  });
  const base = await start(t, app);
  const responses = await Promise.all([fetch(base), fetch(base)]);
  const bodies = await Promise.all(responses.map(response => response.json()));
  assert.deepEqual(bodies, Array.from({ length: 2 }, () => ({
    rows: [{ at: '2030-01-01T00:00:00.000Z', payload: { count: 3 } }], text: '<>&',
  })));
  assert.equal(copies.length, 2);
  assert.notEqual(copies[0], copies[1]);
  assert.equal(value.rows[0]!.payload.count, 2);
  assert.equal(value.rows[0]!.at.getUTCFullYear(), 2026);
  assert.deepEqual(authorityCounts, [2, 2]);
  app.set('json replacer', undefined);
  assert.equal(await (await fetch(base)).text(), encoded, 'custom mutations cannot change the immutable default body');
  assert.equal(copies.length, 2);
  for (const [setting, configured] of [['json spaces', 2], ['json escape', true]] as const) {
    app.set(setting, configured);
    const response = await fetch(base);
    assert.deepEqual(await response.json(), JSON.parse(encoded));
    assert.equal(copies.length, setting === 'json spaces' ? 3 : 4, `${setting} also gets a private delivery copy`);
    app.set(setting, undefined);
  }
});

test('delivery settings are read after the final privacy fence and before cloning', async t => {
  let reached!: () => void, release!: () => void, reads = 0, clones = 0;
  const waiting = new Promise<void>(resolve => { reached = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  t.after(() => release());
  const value = { nested: { visible: true } };
  const marker = PreparedJson(value, JSON.stringify(value), original => {
    assert.equal(reads, 2);
    clones++;
    return structuredClone(original);
  });
  const app = express();
  app.get('/', async (_req, res) => {
    await publicJson(res, async () => {
      if (++reads === 2) { reached(); await gate; }
      return 1;
    }, async () => marker);
  });
  const base = await start(t, app);
  const pending = fetch(base);
  await waiting;
  assert.equal(clones, 0);
  app.set('json spaces', 2);
  release();
  const response = await pending;
  assert.equal(await response.text(), JSON.stringify(value, null, 2));
  assert.equal(clones, 1);
});

test('a failed delivery clone does not poison another delivery of the same prepared value', async t => {
  let clones = 0;
  const value = { visible: true };
  const marker = PreparedJson(value, JSON.stringify(value), original => {
    if (++clones === 1) throw new Error('one delivery failed');
    return structuredClone(original);
  });
  const app = express(); app.set('json spaces', 2);
  app.get('/', async (_req, res) => {
    try { await publicJson(res, async () => 1, async () => marker); }
    catch { res.status(503).json({ error: 'Unavailable' }); }
  });
  const base = await start(t, app);
  const responses = await Promise.all([fetch(base), fetch(base)]);
  assert.deepEqual(responses.map(response => response.status).sort(), [200, 503]);
  assert.deepEqual(await responses.find(response => response.status === 200)!.json(), value);
  await responses.find(response => response.status === 503)!.text();
  assert.equal(clones, 2);
});

test('two-argument prepared custom delivery preserves class and toJSON behavior', async t => {
  class Value {
    calls = 0;
    toJSON() { return { serialization: ++this.calls }; }
  }
  const value = new Value();
  const marker = PreparedJson(value, JSON.stringify(value));
  const app = express(); app.set('json spaces', 2);
  app.get('/', async (_req, res) => {
    await publicJson(res, async () => 1, async () => marker);
  });
  const base = await start(t, app);
  assert.deepEqual(await (await fetch(base)).json(), { serialization: 2 });
  assert.deepEqual(await (await fetch(base)).json(), { serialization: 3 });
  assert.equal(value.calls, 3);
});

test('ordinary payload properties cannot masquerade as an encoded representation', async t => {
  const value = { value: { visible: true }, encodedBody: '{"forged":true}' };
  const app = express();
  app.get('/', async (_req, res) => { await publicJson(res, async () => 1, async () => value); });
  const base = await start(t, app);
  assert.deepEqual(await (await fetch(base)).json(), value);
});

test('builtin ETag is computed once while each fenced delivery owns fresh UTF8, including HEAD and304', async t => {
  const app = express();
  const value = { text: 'Public 🛰️ <>& "node" ' + 'x'.repeat(1200) };
  const body = JSON.stringify(value), marker = PreparedJson(value, body, structuredClone);
  const original = Buffer.from, createHash = crypto.createHash;
  const deliveries: Buffer[] = [];
  let hashes = 0, reads = 0, release!: () => void, reached!: () => void;
  const waiting = new Promise<void>(resolve => { reached = resolve; });
  const gate = new Promise<void>(resolve => { release = resolve; });
  t.mock.method(Buffer, 'from', (value: unknown, ...options: unknown[]) => {
    const buffer = Reflect.apply(original, Buffer, [value, ...options]);
    if (value === body) deliveries.push(buffer);
    return buffer;
  });
  t.mock.method(crypto, 'createHash', (...args: Parameters<typeof createHash>) => {
    if (args[0] === 'sha1') hashes++;
    return Reflect.apply(createHash, crypto, args);
  });
  app.get('/', async (_req, res) => {
    let count = 0;
    await publicJson(res, async () => {
      reads++; count++;
      if (reads === 2 && count === 2) { reached(); await gate; }
      return 1;
    }, async () => marker);
  });
  const base = await start(t, app);
  t.after(() => release());
  const pending = fetch(base);
  await waiting;
  assert.equal(deliveries.length, 0, 'no bytes allocated before final authority completes');
  assert.equal(hashes, 0, 'no validator computed before final authority completes');
  release();
  const first = await pending;
  assert.equal(await first.text(), body);
  assert.equal(first.headers.get('content-type'), 'application/json; charset=utf-8');
  assert.equal(first.headers.get('content-length'), String(Buffer.byteLength(body)));
  const etag = first.headers.get('etag')!;
  assert.ok(etag);
  assert.equal(await (await fetch(base)).text(), body);
  const head = await fetch(base, { method: 'HEAD' });
  assert.equal(await head.text(), '');
  assert.equal(head.headers.get('etag'), etag);
  assert.equal(head.headers.get('content-length'), first.headers.get('content-length'));
  const cached = await fetch(base, { cache: 'force-cache', headers: { 'If-None-Match': etag } });
  assert.equal(cached.status, 304); assert.equal(await cached.text(), '');
  assert.equal(reads, 8); assert.equal(deliveries.length, 4);
  assert.equal(new Set(deliveries).size, 4, 'no delivery shares mutable bytes');
  assert.equal(hashes, 1, 'HEAD and conditional delivery reuse only the builtin validator');
});

test('custom ETag mutation through either setting cannot poison a shared prepared value', async t => {
  const app = express(), value = { text: 'visible 🛰️ ' + 'x'.repeat(1200) };
  const body = JSON.stringify(value), marker = PreparedJson(value, body);
  app.get('/ordinary', (_req, res) => res.json(value));
  app.get('/prepared', async (_req, res) => publicJson(res, async () => 1, async () => marker));
  const base = await start(t, app);
  assert.equal(await (await fetch(base + '/prepared')).text(), body, 'populate the builtin validator');
  for (const setting of ['etag', 'etag fn']) {
    app.set('etag', 'weak');
    const received: Buffer[] = [];
    const mutate = (chunk: Buffer) => {
      assert.ok(Buffer.isBuffer(chunk));
      received.push(chunk);
      const index = chunk.indexOf('visible'); assert.ok(index >= 0);
      chunk[index] = 'V'.charCodeAt(0);
      return '"custom-mutating-validator"';
    };
    app.set(setting, mutate);
    if (setting === 'etag fn') assert.equal(app.get('etag'), 'weak', 'direct fn override leaves ordinary setting unchanged');
    const control = await fetch(base + '/ordinary'), prepared = await fetch(base + '/prepared');
    const controlBody = await control.text(), preparedBody = await prepared.text();
    assert.equal(preparedBody, controlBody); assert.notEqual(preparedBody, body);
    assert.equal(prepared.headers.get('etag'), control.headers.get('etag'));
    assert.equal(received.length, 2); assert.notEqual(received[0], received[1], 'hooks get fresh per-delivery buffers');
    app.set('etag', 'weak');
    assert.equal(await (await fetch(base + '/prepared')).text(), body, 'custom hook cannot corrupt later delivery');
  }
});

test('builtin validator changes recompute only the last weak or strong ETag', async t => {
  const app = express(), value = { text: 'Public 🛰️ <>& ' + 'xyz'.repeat(700) };
  const body = JSON.stringify(value), marker = PreparedJson(value, body);
  const createHash = crypto.createHash;
  let hashes = 0;
  t.mock.method(crypto, 'createHash', (...args: Parameters<typeof createHash>) => {
    if (args[0] === 'sha1') hashes++;
    return Reflect.apply(createHash, crypto, args);
  });
  app.get('/ordinary', (_req, res) => res.json(value));
  app.get('/prepared', async (_req, res) => publicJson(res, async () => 1, async () => marker));
  const base = await start(t, app);
  let previous = '';
  for (const mode of ['weak', 'weak', 'strong', 'strong', 'weak']) {
    app.set('etag', mode);
    const control = await fetch(base + '/ordinary');
    assert.equal(await control.text(), body);
    const before = hashes, prepared = await fetch(base + '/prepared');
    assert.equal(await prepared.text(), body);
    assert.equal(prepared.headers.get('etag'), control.headers.get('etag'));
    assert.equal(prepared.headers.get('etag')!.startsWith('W/'), mode === 'weak');
    assert.equal(hashes - before, mode === previous ? 0 : 1);
    previous = mode;
  }
});

test('preset ETag retains ordinary string delivery and conditional semantics', async t => {
  const app = express(), value = { text: 'small 🛰️' }, body = JSON.stringify(value);
  const marker = PreparedJson(value, body), kinds: string[] = [];
  app.use((req, res, next) => {
    res.setHeader('ETag', '"application-owned"');
    if (req.path === '/prepared') {
      const end = res.end;
      res.end = function (chunk: unknown, ...args: unknown[]) {
        kinds.push(typeof chunk); return Reflect.apply(end, this, [chunk, ...args]);
      } as typeof res.end;
    }
    next();
  });
  app.get('/ordinary', (_req, res) => res.json(value));
  app.get('/prepared', async (_req, res) => publicJson(res, async () => 1, async () => marker));
  const base = await start(t, app);
  const control = await fetch(base + '/ordinary'), prepared = await fetch(base + '/prepared');
  assert.equal(await prepared.text(), await control.text());
  for (const name of ['content-type', 'content-length', 'etag']) assert.equal(prepared.headers.get(name), control.headers.get(name));
  assert.deepEqual(kinds, ['string']);
  const conditional = { cache: 'force-cache' as const, headers: { 'If-None-Match': '"application-owned"' } };
  assert.equal((await fetch(base + '/ordinary', conditional)).status, 304);
  assert.equal((await fetch(base + '/prepared', conditional)).status, 304);
});

test('custom header setters observe ordinary Content-Length before ETag assignment', async t => {
  for (const method of ['set', 'setHeader'] as const) await t.test(method, async child => {
    const app = express(), value = { text: '🛰️ public ' + 'xyz'.repeat(700) };
    const body = JSON.stringify(value), marker = PreparedJson(value, body);
    const observed: Array<{ route: string; length: unknown; type: unknown }> = [];
    app.use((req, res, next) => {
      const setter = res[method];
      Object.defineProperty(res, method, { configurable: true, value: function (name: unknown, ...args: unknown[]) {
        if (typeof name === 'string' && name.toLowerCase() === 'etag') {
          observed.push({ route: req.path, length: this.getHeader('Content-Length'), type: this.getHeader('Content-Type') });
        }
        return Reflect.apply(setter, this, [name, ...args]);
      } });
      next();
    });
    app.get('/ordinary', (_req, res) => res.json(value));
    app.get('/prepared', async (_req, res) => publicJson(res, async () => 1, async () => marker));
    const base = await start(child, app);
    const control = await fetch(base + '/ordinary'), prepared = await fetch(base + '/prepared');
    assert.equal(await prepared.text(), await control.text());
    assert.equal(prepared.headers.get('etag'), control.headers.get('etag'));
    assert.deepEqual(observed.map(({ route, ...headers }) => headers), [
      { length: String(Buffer.byteLength(body)), type: 'application/json; charset=utf-8' },
      { length: String(Buffer.byteLength(body)), type: 'application/json; charset=utf-8' },
    ]);
  });
});

test('custom header queries retain logical presets and ordinary query behavior', async t => {
  for (const method of ['get', 'getHeader', 'hasHeader'] as const) await t.test(method, async child => {
    const app = express(), value = { text: 'small 🛰️' }, body = JSON.stringify(value);
    const marker = PreparedJson(value, body);
    app.use((_req, res, next) => {
      const original = res[method];
      Object.defineProperty(res, method, { configurable: true, value: function (name: string) {
        if (method === 'hasHeader') throw new Error('ordinary send does not query hasHeader');
        if (name.toLowerCase() === 'etag') return '"logical-preset"';
        return Reflect.apply(original, this, [name]);
      } });
      next();
    });
    app.get('/ordinary', (_req, res) => res.json(value));
    app.get('/prepared', async (_req, res) => publicJson(res, async () => 1, async () => marker));
    const base = await start(child, app);
    const control = await fetch(base + '/ordinary'), prepared = await fetch(base + '/prepared');
    assert.equal(control.status, 200); assert.equal(prepared.status, 200);
    assert.equal(await prepared.text(), await control.text());
    for (const name of ['content-type', 'content-length', 'etag']) assert.equal(prepared.headers.get(name), control.headers.get(name));
    if (method !== 'hasHeader') assert.equal(prepared.headers.get('etag'), null, 'logical preset prevents generation without writing a new header');
  });
});

test('custom application getter retains original lookup count and order', async t => {
  const app = express(), value = { text: 'Public 🛰️ ' + 'xyz'.repeat(700) }, body = JSON.stringify(value);
  const marker = PreparedJson(value, body);
  let lookups: string[] = [];
  app.get('/ordinary', (_req, res) => res.json(value));
  app.get('/prepared', async (_req, res) => publicJson(res, async () => 1, async () => marker));
  const get = app.get;
  app.get = function (name: string, ...args: unknown[]) {
    if (!args.length) lookups.push(name);
    return Reflect.apply(get, this, [name, ...args]);
  } as typeof app.get;
  const base = await start(t, app);
  lookups = [];
  const control = await fetch(base + '/ordinary'); assert.equal(await control.text(), body);
  const expected = [...lookups];
  lookups = [];
  const prepared = await fetch(base + '/prepared'); assert.equal(await prepared.text(), body);
  const relevant = (names: string[]) => names.filter(name => name.startsWith('json ') || name === 'etag fn');
  assert.deepEqual(relevant(expected), ['json escape', 'json replacer', 'json spaces', 'etag fn']);
  // PreparedJson's existing settings order differs from res.json, but its only
  // etag lookup still belongs to ordinary send after those settings reads.
  assert.deepEqual(relevant(lookups), ['json replacer', 'json spaces', 'json escape', 'etag fn']);
  assert.equal(lookups.filter(name => name === 'etag fn').length, 1);
  assert.equal(prepared.headers.get('etag'), control.headers.get('etag'));
});

test('overridden send receives the original string, preserving preset content types', async t => {
  const app = express(), value = { text: 'public 🛰️ ' + 'x'.repeat(1200) };
  const body = JSON.stringify(value), marker = PreparedJson(value, body);
  const seen: unknown[] = [];
  app.use((req, res, next) => {
    if (req.query.preset) res.type('application/vnd.ukmesh.map+json');
    if (req.path === '/prepared') {
      const send = res.send;
      res.send = function (body) { seen.push(body); return send.call(this, body); };
    }
    next();
  });
  app.get('/ordinary', (_req, res) => res.json(value));
  app.get('/prepared', async (_req, res) => publicJson(res, async () => 1, async () => marker));
  const base = await start(t, app);
  for (const query of ['', '?preset=1']) {
    const control = await fetch(base + '/ordinary' + query), prepared = await fetch(base + '/prepared' + query);
    assert.equal(await prepared.text(), await control.text());
    for (const header of ['content-type', 'content-length', 'etag']) assert.equal(prepared.headers.get(header), control.headers.get(header));
  }
  assert.deepEqual(seen, [body, body]);
});

test('production compression preserves exact gzip bytes, validators, HEAD and304', async t => {
  const { default: compression } = await import('compression');
  const { default: http } = await import('node:http');
  const { gunzipSync } = await import('node:zlib');
  const app = express(), value = { text: 'Public 🛰️ <>& ' + 'xyz'.repeat(1500) };
  const body = JSON.stringify(value), marker = PreparedJson(value, body);
  let reads = 0;
  app.use(compression());
  app.get('/ordinary', (_req, res) => res.json(value));
  app.get('/prepared', async (_req, res) => {
    reads = 0;
    await publicJson(res, async () => { reads++; return 1; }, async () => marker);
  });
  const base = await start(t, app);
  const request = (route: string, method = 'GET', headers: Record<string, string> = {}) =>
    new Promise<{ status: number; headers: import('node:http').IncomingHttpHeaders; body: Buffer }>((resolve, reject) => {
      const req = http.request(base + route, { method, headers: { 'Accept-Encoding': 'gzip', ...headers } }, res => {
        const chunks: Buffer[] = [];
        res.on('data', chunk => chunks.push(chunk)); res.on('error', reject);
        res.on('end', () => resolve({ status: res.statusCode!, headers: res.headers, body: Buffer.concat(chunks) }));
      });
      req.on('error', reject); req.end();
    });
  const control = await request('/ordinary'), prepared = await request('/prepared');
  assert.equal(control.headers['content-encoding'], 'gzip');
  assert.deepEqual(prepared.body, control.body); assert.equal(gunzipSync(prepared.body).toString(), body);
  for (const header of ['content-type', 'content-encoding', 'content-length', 'etag', 'vary']) assert.equal(prepared.headers[header], control.headers[header]);
  assert.equal(reads, 2);
  for (const [method, headers] of [['HEAD', {}], ['GET', { 'If-None-Match': String(control.headers.etag) }]] as const) {
    const ordinary = await request('/ordinary', method, headers), actual = await request('/prepared', method, headers);
    assert.equal(actual.status, method === 'HEAD' ? 200 : 304); assert.equal(actual.status, ordinary.status);
    assert.equal(actual.body.length, 0); assert.equal(reads, 2);
    for (const header of ['content-type', 'content-encoding', 'content-length', 'etag', 'vary']) assert.equal(actual.headers[header], ordinary.headers[header]);
  }
});

test('custom end mutation under compression preserves repeated-marker ownership', async t => {
  const { default: compression } = await import('compression');
    const app = express(), value = { text: 'visible 🛰️ ' + 'xyz'.repeat(1000) };
    const body = JSON.stringify(value), marker = PreparedJson(value, body);
    app.use(compression());
    app.use((req, res, next) => {
      if (req.query.mutate) {
        const end = res.end;
        res.end = function (chunk: unknown, ...args: unknown[]) {
          assert.ok(Buffer.isBuffer(chunk));
          const index = chunk.indexOf('visible'); assert.ok(index >= 0);
          chunk[index] = 'V'.charCodeAt(0);
          return Reflect.apply(end, this, [chunk, ...args]);
        } as typeof res.end;
      }
      next();
    });
    app.get('/ordinary', (_req, res) => res.json(value));
    app.get('/prepared', async (_req, res) => publicJson(res, async () => 1, async () => marker));
    const base = await start(t, app);
    assert.equal(await (await fetch(base + '/prepared')).text(), body);
    const ordinary = await fetch(base + '/ordinary?mutate=1'), actual = await fetch(base + '/prepared?mutate=1');
    const expected = await ordinary.text();
    assert.notEqual(expected, body); assert.equal(await actual.text(), expected);
    assert.equal(actual.headers.get('etag'), ordinary.headers.get('etag'));
    assert.equal(await (await fetch(base + '/prepared')).text(), body, 'transport mutation cannot poison later delivery');
});

test('small UTF16 strings retain original end input even when UTF8 exceeds1000bytes', async t => {
  const app = express(); app.disable('etag');
  const value = { text: '🛰️'.repeat(200) }, body = JSON.stringify(value);
  assert.ok(body.length < 1000); assert.ok(Buffer.byteLength(body) > 1000);
  const marker = PreparedJson(value, body), kinds: string[] = [];
  app.use((_req, res, next) => {
    const end = res.end;
    res.end = function (chunk: unknown, ...args: unknown[]) {
      kinds.push(typeof chunk); return Reflect.apply(end, this, [chunk, ...args]);
    } as typeof res.end;
    next();
  });
  app.get('/ordinary', (_req, res) => res.json(value));
  app.get('/prepared', async (_req, res) => publicJson(res, async () => 1, async () => marker));
  const base = await start(t, app);
  assert.equal(await (await fetch(base + '/ordinary')).text(), body);
  assert.equal(await (await fetch(base + '/prepared')).text(), body);
  assert.deepEqual(kinds, ['string', 'string']);
});
