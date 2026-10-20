import assert from 'node:assert/strict';
import test from 'node:test';
import { MeshCoreDecoder } from '@michaelhart/meshcore-decoder';
import { decodePacketCompat } from '../mqtt/decodePacket.js';
import { REGION_NAMES } from './regionNames.js';
import { decodeRegionForPacket, transportCode0Of } from './scopeDecode.js';

// Real route-type 0/3 frames captured from the ukmesh corpus. Each was
// verified against the live database region_scope column and the
// @michaelhart/meshcore-decoder library during the 2026-10 region-scope audit.
const SAMPLES: Array<{ region: string; raw: string }> = [
  {
    region: 'eng-ne',
    raw: '04313C000081F51858D28648D4184042345B3FD371B1E3B17B1D86B1F7',
  },
  {
    region: 'yorkshire',
    raw: '14CD3D0000854D52E4A8AB2FBBC5B1BBDE0C8C197F805A2BC47E8F027BED00573888281E957B3DEE0496717D73274BD5877B39047BE57D9FE91E09E08345F9818829DDD1750C3D3CC44A708ECB7931CE6DAA2275E9E11F386D677348E375CD419A327D6AC77EE68267BC1FB08CC4732EEDB6EED385D9764022ADC98C5397DA100AFB4A8160A6DAABB59806C96E4C0D9CF0C66948404FF8FA',
  },
  {
    region: 'eng-yh',
    raw: '08F5FE0000818C197F65378C55A3A9D3BE9347E808CA4D012626A27E4701E12EAE0210BCBD0F4C7EC378552F9A',
  },
  {
    region: 'gb',
    raw: '141E380000416D11118EEA9A9C8AAE7EA6AB13A622CCA86A3BFD1CD8F98FC7DF8FF5FE3344DF7B230A6A0891362A348567669EE0A7A3F9229884F51D4A6A8EA833999DA6E5D36B16CDC1FA3C52D391D8BBECE77280C26A70A3C7804FAB3F343A1342EF8CA0C2423A10498CBE460D3C3503546D303E5102037FB87A3C57E4673B576FEAD716ED94756EE7F1',
  },
];

// Documented MeshCore public channel material, not an application credential.
const keyStore = MeshCoreDecoder.createKeyStore({
  channelSecrets: ['8b3387e9c5cdea6ac9e5edbaa115cd72'],
});

test('transportCode0Of reads the first little-endian uint16 and rejects malformed input', () => {
  assert.equal(transportCode0Of('CD3D0000'), 0x3dcd);
  assert.equal(transportCode0Of('01FF'), 0xff01);
  assert.equal(transportCode0Of(''), null);
  assert.equal(transportCode0Of('CD3'), null);
  assert.equal(transportCode0Of('ZZZZ'), null);
});

test('decodeRegionForPacket attributes captured scoped packets to their region', () => {
  for (const sample of SAMPLES) {
    const compat = decodePacketCompat(sample.raw, keyStore);
    assert.equal(compat.metadataValid, true, `${sample.region}: frame must decode cleanly`);
    assert.ok(compat.transportCodes, `${sample.region}: transport codes must be present`);
    const tc0 = transportCode0Of(compat.transportCodes);
    assert.notEqual(tc0, null);
    const decoded = compat.decoded;
    assert.ok(decoded?.payload?.raw, `${sample.region}: payload raw must be present`);
    const name = decodeRegionForPacket(
      REGION_NAMES,
      decoded.payloadType,
      decoded.payload.raw,
      tc0,
    );
    assert.equal(name, sample.region);
  }
});

test('decodeRegionForPacket returns null when no candidate matches or inputs are missing', () => {
  const [first] = SAMPLES;
  const compat = decodePacketCompat(first.raw, keyStore);
  assert.ok(compat.decoded?.payload?.raw);
  const tc0 = transportCode0Of(compat.transportCodes!);
  assert.equal(
    decodeRegionForPacket(['definitely-not-a-region'], compat.decoded.payloadType, compat.decoded.payload.raw, tc0),
    null,
  );
  assert.equal(decodeRegionForPacket(REGION_NAMES, null, compat.decoded.payload.raw, tc0), null);
  assert.equal(decodeRegionForPacket(REGION_NAMES, compat.decoded.payloadType, null, tc0), null);
  assert.equal(decodeRegionForPacket(REGION_NAMES, compat.decoded.payloadType, compat.decoded.payload.raw, null), null);
});
