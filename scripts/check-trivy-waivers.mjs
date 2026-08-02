#!/usr/bin/env node

import { readFileSync } from 'node:fs';

const path = process.argv[2] ?? '.trivyignore.yaml';
const text = readFileSync(path, 'utf8');
const lines = text.split(/\r?\n/);
const allowedSections = new Set([
  'vulnerabilities',
  'misconfigurations',
  'secrets',
  'licenses',
]);

let section = '';
let current = null;
const entries = [];

function finishEntry() {
  if (current) {
    entries.push(current);
    current = null;
  }
}

for (let index = 0; index < lines.length; index += 1) {
  const raw = lines[index];
  const trimmed = raw.trim();
  if (!trimmed || trimmed.startsWith('#')) continue;

  const sectionMatch = raw.match(/^([a-z_]+):(?:\s*\[\])?\s*$/);
  if (sectionMatch) {
    finishEntry();
    section = sectionMatch[1];
    if (!allowedSections.has(section)) {
      throw new Error(`${path}:${index + 1}: unsupported waiver section ${section}`);
    }
    continue;
  }

  const entryMatch = raw.match(/^\s{2}-\s+id:\s*(\S.*)$/);
  if (entryMatch) {
    if (!section) {
      throw new Error(`${path}:${index + 1}: waiver entry has no section`);
    }
    finishEntry();
    current = { section, line: index + 1, id: entryMatch[1].trim() };
    continue;
  }

  const fieldMatch = raw.match(/^\s{4}([a-z_]+):\s*(.*)$/);
  if (fieldMatch && current) {
    current[fieldMatch[1]] = fieldMatch[2].trim().replace(/^['"]|['"]$/g, '');
    continue;
  }

  // List values such as paths and purls are accepted by Trivy and are scoped
  // by the enclosing key, but do not affect the policy fields checked here.
  if (/^\s{6}-\s+\S/.test(raw) && current) continue;

  throw new Error(`${path}:${index + 1}: unrecognised waiver syntax`);
}
finishEntry();

const now = new Date();
const maximumLifetimeMs = 30 * 24 * 60 * 60 * 1000;
const seen = new Set();
for (const entry of entries) {
  const key = `${entry.section}:${entry.id}`;
  if (seen.has(key)) {
    throw new Error(`${path}:${entry.line}: duplicate waiver ${key}`);
  }
  seen.add(key);

  if (!entry.statement || entry.statement.length < 20) {
    throw new Error(`${path}:${entry.line}: ${key} needs a substantive statement`);
  }
  if (!/\bowner=[^;\s]+/.test(entry.statement)) {
    throw new Error(`${path}:${entry.line}: ${key} statement must contain owner=<name>`);
  }
  if (!/\breviewed=\d{4}-\d{2}-\d{2}\b/.test(entry.statement)) {
    throw new Error(`${path}:${entry.line}: ${key} statement must contain reviewed=YYYY-MM-DD`);
  }
  if (!entry.expired_at) {
    throw new Error(`${path}:${entry.line}: ${key} needs expired_at`);
  }
  const expiry = new Date(`${entry.expired_at}T23:59:59Z`);
  if (Number.isNaN(expiry.getTime())) {
    throw new Error(`${path}:${entry.line}: ${key} has an invalid expired_at`);
  }
  if (expiry <= now) {
    throw new Error(`${path}:${entry.line}: ${key} expired on ${entry.expired_at}`);
  }

  const reviewedMatch = entry.statement.match(/\breviewed=(\d{4}-\d{2}-\d{2})\b/);
  const reviewed = new Date(`${reviewedMatch[1]}T00:00:00Z`);
  if (Number.isNaN(reviewed.getTime()) || reviewed > now) {
    throw new Error(`${path}:${entry.line}: ${key} has an invalid reviewed date`);
  }
  if (expiry.getTime() - reviewed.getTime() > maximumLifetimeMs) {
    throw new Error(`${path}:${entry.line}: ${key} exceeds the 30-day waiver lifetime`);
  }
}

console.log(`Validated ${entries.length} active Trivy waiver(s) in ${path}.`);
