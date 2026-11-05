#!/usr/bin/env node
/**
 * Guard against database-backed audit tests silently skipping.
 *
 * The `.audit.test.ts` files only run when a loopback fixture database is
 * supplied; without it they report `# SKIP` and node:test still exits 0. That
 * makes a missing or misconfigured CI service indistinguishable from a green
 * run, so every invocation of these files is piped through this check.
 *
 * Usage: node scripts/assert-audit-tests-ran.mjs <tap-file> [label] [--min-tests=N]
 *
 * Exits non-zero when the TAP stream shows any skip, todo, cancelled or failed
 * test, when the counts disagree, or when fewer than `--min-tests` tests ran.
 */
import { readFileSync } from 'node:fs';

const args = process.argv.slice(2);
const minTestsArg = args.find((arg) => arg.startsWith('--min-tests='));
const positional = args.filter((arg) => !arg.startsWith('--'));
const [tapPath, label = tapPath] = positional;
const minTests = minTestsArg ? Number(minTestsArg.split('=')[1]) : 1;

if (!tapPath) {
  console.error('usage: assert-audit-tests-ran.mjs <tap-file> [label] [--min-tests=N]');
  process.exit(2);
}
if (!Number.isInteger(minTests) || minTests < 1) {
  console.error(`invalid --min-tests value: ${minTestsArg}`);
  process.exit(2);
}

let tap;
try {
  tap = readFileSync(tapPath, 'utf8');
} catch (error) {
  console.error(`${label}: no TAP output at ${tapPath} (${error.code ?? error.message})`);
  process.exit(1);
}

const lines = tap.split('\n');
const summary = new Map();
for (const line of lines) {
  const match = /^#\s+(tests|pass|fail|cancelled|skipped|todo)\s+(\d+)\s*$/.exec(line);
  if (match) summary.set(match[1], Number(match[2]));
}

const required = ['tests', 'pass', 'fail', 'skipped'];
const missing = required.filter((key) => !summary.has(key));
if (missing.length > 0) {
  console.error(`${label}: TAP stream has no summary (missing ${missing.join(', ')}); the runner did not complete`);
  process.exit(1);
}

const tests = summary.get('tests');
const pass = summary.get('pass');
const fail = summary.get('fail');
const skipped = summary.get('skipped');
const cancelled = summary.get('cancelled') ?? 0;
const todo = summary.get('todo') ?? 0;

const problems = [];
if (skipped > 0) problems.push(`${skipped} test(s) skipped — the audit fixture database was not usable`);
if (todo > 0) problems.push(`${todo} test(s) marked todo`);
if (cancelled > 0) problems.push(`${cancelled} test(s) cancelled`);
if (fail > 0) problems.push(`${fail} test(s) failed`);
if (pass !== tests) problems.push(`pass (${pass}) does not equal tests (${tests})`);
if (tests < minTests) problems.push(`only ${tests} test(s) ran, expected at least ${minTests}`);

const skipReasons = lines.filter((line) => /^ok\s+\d+\s+.*#\s*(SKIP|TODO)/i.test(line));
if (skipReasons.length > 0) {
  console.error(`${label}: skipped/todo entries:`);
  for (const line of skipReasons) console.error(`  ${line.trim()}`);
}

const failed = lines.filter((line) => /^not ok\s+\d+/.test(line));
if (failed.length > 0) {
  console.error(`${label}: failing entries:`);
  for (const line of failed) console.error(`  ${line.trim()}`);
}

const counts = `tests=${tests} pass=${pass} fail=${fail} skipped=${skipped} cancelled=${cancelled} todo=${todo}`;
if (problems.length > 0) {
  console.error(`${label}: ${counts}`);
  for (const problem of problems) console.error(`  - ${problem}`);
  process.exit(1);
}

console.log(`${label}: ${counts}`);
