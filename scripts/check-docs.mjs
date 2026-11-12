#!/usr/bin/env node
/** Static living-document checks. Never executes runbook commands. */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const rootArgument = process.argv.indexOf('--root');
const root = rootArgument < 0 ? path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
  : path.resolve(process.argv[rootArgument + 1]);
// Explicitly classified review evidence predating the current release audit.
const historicalReports = new Set(['docs/security-closure.md']);
const historical = (name) => historicalReports.has(name) || name.startsWith('docs/reviews/') || /(?:^|\/)\S*\d{4}-\d{2}-\d{2}\S*\.md$/.test(name);
function walk(directory) {
  return fs.readdirSync(path.join(root, directory), { withFileTypes: true }).flatMap((entry) => {
    const name = path.posix.join(directory, entry.name);
    return entry.isDirectory() ? walk(name) : name.endsWith('.md') ? [name] : [];
  });
}
const documents = ['README.md', 'INSTALL.md', ...(fs.existsSync(path.join(root, 'CHANGELOG.md')) ? ['CHANGELOG.md'] : []), ...walk('docs')].filter((name) => !historical(name));
const issues = [];
const cache = new Map();
const stale = new Map([
  ['backend/src/db/ownerAuth.ts', 'backend/src/owner/ownerAuth.ts'],
  ['backend/src/api/utils/networkFilters', 'backend/src/platform/query/networkFilters'],
  ['backend/src/mqtt/observationIdentity', 'backend/src/db/observationIdentity'],
  ['path-lazy/lazyResolverLegacy', 'the current pathing harness in docs/pathing.md'],
  ['compose.phase4.yml', 'docker-compose.yml'],
  ['docker-compose.infra-client.yml', 'docker-compose.yml'],
]);
function withoutFences(text) { return text.replace(/^(`{3,}|~{3,})[^\n]*\n[\s\S]*?^\1[^\n]*$/gm, ''); }
function anchors(filename) {
  if (cache.has(filename)) return cache.get(filename);
  const text = withoutFences(fs.readFileSync(filename, 'utf8'));
  const values = new Set();
  const duplicates = new Map();
  for (const match of text.matchAll(/^ {0,3}#{1,6}\s+(.+?)\s*#*\s*$/gm)) {
    const slug = match[1].replace(/<[^>]*>/g, '').replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
      .replace(/[`*_~]/g, '').toLowerCase().replace(/[^\p{L}\p{N}\p{M}\-_ ]/gu, '').replace(/ /g, '-');
    const count = duplicates.get(slug) ?? 0;
    values.add(count ? `${slug}-${count}` : slug);
    duplicates.set(slug, count + 1);
  }
  for (const match of text.matchAll(/\b(?:id|name)=["']([^"']+)["']/g)) values.add(match[1]);
  cache.set(filename, values);
  return values;
}
function checkLink(document, destination) {
  const raw = destination.trim().replace(/^<([^>]+)>$/, '$1').replace(/\s+["'][\s\S]*$/, '');
  if (!raw || /^(?:[a-z][\w+.-]*:|\/\/)/i.test(raw) || raw.includes('${')) return;
  const [pathname, fragment] = raw.split('#');
  let decoded;
  try { decoded = decodeURIComponent(pathname.split('?')[0]); } catch { return; }
  // Leading / links refer to deployed application routes, not repo files.
  if (decoded.startsWith('/')) return;
  const target = decoded ? path.resolve(root, path.dirname(document), decoded) : path.join(root, document);
  if (!target.startsWith(root + path.sep)) { issues.push(`${document}: link leaves repository: ${raw}`); return; }
  if (!fs.existsSync(target)) { issues.push(`${document}: missing relative link: ${raw}`); return; }
  if (fragment && target.endsWith('.md') && !historical(path.relative(root, target))
    && !anchors(target).has(decodeURIComponent(fragment))) issues.push(`${document}: missing anchor: ${raw}`);
}

for (const document of documents) {
  const text = fs.readFileSync(path.join(root, document), 'utf8');
  const prose = withoutFences(text);
  for (const match of prose.matchAll(/!?\[[^\]\n]*\]\((<[^>]+>|[^\s)]+)(?:\s+["'][^\n]*?["'])?\)/g)) checkLink(document, match[1]);
  for (const match of prose.matchAll(/^\s*\[[^\]]+\]:\s*(\S+)/gm)) checkLink(document, match[1]);
  for (const [old, replacement] of stale) {
    // A line explicitly labelled historical/deprecated/removed may quote a
    // former path; current instructions may not depend on it.
    for (const line of text.split('\n')) {
      if (line.includes(old) && !/historical|deprecated|removed|deleted|superseded|formerly/i.test(line)) {
        issues.push(`${document}: stale path ${old}; use ${replacement}`);
      }
    }
  }
  for (const block of text.matchAll(/^```(?:bash|sh|shell|console)\s*\n([\s\S]*?)^```/gm)) {
    const commands = block[1].replace(/\\\r?\n\s*/g, ' ');
    for (const line of commands.split('\n')) {
      if (line.trimStart().startsWith('#')) continue;
      if (/\bdocker\s+(?:compose\s+[^\n]*?\b(?:exec|run)\s+[^\n]*?\b(?:backend|db-migrate)|exec\s+(?:-[^\s]+\s+)*meshcore-analytics-backend-\d+)\s+npm\b/.test(line)) {
        issues.push(`${document}: backend runtime image has no npm; invoke its compiled Node tool directly`);
      }
    }
    let working = root;
    for (const line of block[1].split('\n')) {
      const cd = /(?:^|&&\s*)cd\s+(backend|frontend|scripts)(?:\s|$)/.exec(line.trim());
      if (cd) working = path.join(root, cd[1]);
      if (/^\s*cd\s+\.\.(?:\s|$)/.test(line)) working = path.dirname(working);
      const match = /\bnpm\s+(?:--prefix\s+(backend|frontend|scripts)\s+)?run\s+([\w:.-]+)/.exec(line);
      if (match && !line.trimStart().startsWith('#')) {
        const packageFile = path.join(match[1] ? path.join(root, match[1]) : working, 'package.json');
        // Blocks without a known package cwd are operator examples; avoid
        // guessing their external checkout. Explicit prefixes are checked.
        if (fs.existsSync(packageFile)) {
          const { scripts = {} } = JSON.parse(fs.readFileSync(packageFile, 'utf8'));
          if (!Object.hasOwn(scripts, match[2])) issues.push(`${document}: missing npm script ${match[2]} in ${path.relative(root, packageFile)}`);
        }
      }
    }
  }
}
if (issues.length) {
  console.error(issues.join('\n'));
  process.exitCode = 1;
} else console.log(`Documentation checks passed: ${documents.length} living Markdown documents; historical evidence preserved.`);
