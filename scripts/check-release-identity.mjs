#!/usr/bin/env node
// Static candidate preflight; never builds, publishes or alters an artifact.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = file => readFileSync(path.join(root, file), 'utf8');
const version = JSON.parse(read('backend/package.json')).version;
assert.equal(version, '2.0.0', 'This candidate must identify UKMesh 2.0.0');
for (const component of ['backend', 'frontend', 'scripts']) {
  const manifest = JSON.parse(read(`${component}/package.json`));
  const lock = JSON.parse(read(`${component}/package-lock.json`));
  assert.equal(manifest.version, version, `${component} package version differs`);
  assert.equal(lock.version, version, `${component} lock version differs`);
  assert.equal(lock.packages[''].version, version, `${component} root lock metadata differs`);
}
for (const dockerfile of ['Dockerfile', 'Dockerfile.backend', 'Dockerfile.app',
  'Dockerfile.website', 'Dockerfile.mesh-health-check', 'Dockerfile.mosquitto-reloader',
  'viewshed-worker/Dockerfile', 'tagger-worker/Dockerfile']) {
  const source = read(dockerfile);
  assert.ok(source.includes(`ARG UKMESH_VERSION=${version}\n`), `${dockerfile} default version differs`);
  assert.ok(source.includes('org.opencontainers.image.version="${UKMESH_VERSION}"'), `${dockerfile} lacks package version label`);
  assert.ok(source.includes('org.opencontainers.image.revision="${SOURCE_REVISION}"'), `${dockerfile} lacks source revision label`);
}
for (const file of ['CHANGELOG.md', 'docs/upgrade-2.0.md']) assert.ok(read(file).trim(), `${file} is empty`);
// This same line is also a GitHub Actions output in the image job.
console.log(`version=${version}`);
