import { gzipSync } from 'node:zlib';
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';

const KIB = 1024;
const outputDir = path.resolve('dist/assets');
const files = await readdir(outputDir);

async function measure(matcher) {
  const selected = files.filter(matcher);
  const buffers = await Promise.all(selected.map((file) => readFile(path.join(outputDir, file))));
  return {
    files: selected,
    raw: buffers.reduce((total, buffer) => total + buffer.byteLength, 0),
    gzip: buffers.reduce((total, buffer) => total + gzipSync(buffer).byteLength, 0),
  };
}

const budgets = [
  {
    name: 'main App chunk',
    measure: await measure((file) => /^App-[^.]+\.js$/.test(file)),
    raw: 900 * KIB,
    gzip: 260 * KIB,
    exactlyOne: true,
  },
  {
    name: 'MapLibre chunk',
    measure: await measure((file) => /^maplibre-gl-[^.]+\.js$/.test(file)),
    raw: 820 * KIB,
    gzip: 225 * KIB,
    exactlyOne: true,
  },
  {
    name: 'Recharts categorical chunk',
    measure: await measure((file) => /^generateCategoricalChart-[^.]+\.js$/.test(file)),
    raw: 400 * KIB,
    gzip: 110 * KIB,
    exactlyOne: true,
  },
  {
    name: 'all JavaScript',
    measure: await measure((file) => file.endsWith('.js')),
    raw: 2_900 * KIB,
    gzip: 850 * KIB,
  },
  {
    name: 'all CSS',
    measure: await measure((file) => file.endsWith('.css')),
    raw: 230 * KIB,
    gzip: 42 * KIB,
  },
];

let failed = false;
for (const budget of budgets) {
  const rawKib = budget.measure.raw / KIB;
  const gzipKib = budget.measure.gzip / KIB;
  const missing = budget.exactlyOne && budget.measure.files.length !== 1;
  const exceeds = budget.measure.raw > budget.raw || budget.measure.gzip > budget.gzip;
  const status = missing || exceeds ? 'FAIL' : 'PASS';
  console.log(
    `${status} ${budget.name}: ${rawKib.toFixed(1)} KiB raw / ${gzipKib.toFixed(1)} KiB gzip`
      + ` (limits ${(budget.raw / KIB).toFixed(0)} / ${(budget.gzip / KIB).toFixed(0)})`,
  );
  if (missing) {
    console.error(`Expected exactly one matching asset, found: ${budget.measure.files.join(', ') || 'none'}`);
  }
  failed ||= missing || exceeds;
}

if (failed) process.exitCode = 1;
