import fs from 'node:fs/promises';
import path from 'node:path';
import postcss from 'postcss';

const sourceRoot = path.resolve('src');
const globalsPath = path.join(sourceRoot, 'styles', 'globals.css');
const featureSelector = /^\.(?:owner-|stats-page|uk-feed|feed-|spam-|topology-|map-|node-dock|node-popup|mobile-controls|filter-panel|packet-feed)/;

async function cssFiles(directory) {
  const entries = await fs.readdir(directory, { withFileTypes: true });
  const files = await Promise.all(entries.map(async (entry) => {
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) return cssFiles(target);
    return entry.isFile() && entry.name.endsWith('.css') ? [target] : [];
  }));
  return files.flat().sort();
}

function splitSelectors(value) {
  const selectors = [];
  let start = 0;
  let depth = 0;
  let quote = '';
  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];
    if (quote) {
      if (character === '\\') index += 1;
      else if (character === quote) quote = '';
      continue;
    }
    if (character === '"' || character === "'") {
      quote = character;
    } else if (character === '(' || character === '[') {
      depth += 1;
    } else if (character === ')' || character === ']') {
      depth = Math.max(0, depth - 1);
    } else if (character === ',' && depth === 0) {
      selectors.push(value.slice(start, index));
      start = index + 1;
    }
  }
  selectors.push(value.slice(start));
  return selectors.map((selector) => selector.trim().replace(/\s+/g, ' ')).filter(Boolean);
}

function atRuleContext(rule) {
  const context = [];
  let parent = rule.parent;
  while (parent) {
    if (parent.type === 'atrule') {
      context.unshift(`@${parent.name} ${parent.params}`.trim());
    }
    parent = parent.parent;
  }
  return context.join(' > ') || '<root>';
}

const files = await cssFiles(sourceRoot);
const occurrences = new Map();
const forbiddenGlobals = [];
const undersizedText = [];

for (const file of files) {
  const css = await fs.readFile(file, 'utf8');
  const root = postcss.parse(css, { from: file });
  root.walkDecls((declaration) => {
    let size = null;
    if (declaration.prop === 'font-size') {
      size = declaration.value.match(/^\s*([0-9.]+)(px|rem)\b/);
    } else if (declaration.prop === 'font') {
      size = declaration.value.match(/\b([0-9.]+)(px|rem)(?:\/[^\s]+)?(?=\s)/);
    }
    if (!size) return;
    const pixels = Number(size[1]) * (size[2] === 'rem' ? 16 : 1);
    if (pixels < 10) {
      undersizedText.push({
        file: path.relative(process.cwd(), file),
        line: declaration.source?.start?.line ?? 0,
        pixels,
      });
    }
  });
  root.walkRules((rule) => {
    const context = atRuleContext(rule);
    for (const selector of splitSelectors(rule.selector)) {
      const key = `${context}\u0000${selector}`;
      const list = occurrences.get(key) ?? [];
      list.push({
        file: path.relative(process.cwd(), file),
        line: rule.source?.start?.line ?? 0,
      });
      occurrences.set(key, list);
      if (file === globalsPath && featureSelector.test(selector)) {
        forbiddenGlobals.push({ selector, ...list[list.length - 1] });
      }
    }
  });
}

async function sourceFiles(directory) {
  const entries = await fs.readdir(directory, { withFileTypes: true });
  const files = await Promise.all(entries.map(async (entry) => {
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(target);
    return entry.isFile() && /\.(?:css|html|ts|tsx)$/.test(entry.name) ? [target] : [];
  }));
  return files.flat();
}

const customPropertyDefinitions = new Set();
const customPropertyUsages = new Map();
for (const file of await sourceFiles(sourceRoot)) {
  const source = await fs.readFile(file, 'utf8');
  for (const match of source.matchAll(/(^|[;{\s])(--[a-zA-Z0-9_-]+)\s*:/gm)) {
    customPropertyDefinitions.add(match[2]);
  }
  for (const match of source.matchAll(/var\(\s*(--[a-zA-Z0-9_-]+)\s*(,)?/g)) {
    if (match[2]) continue;
    const usages = customPropertyUsages.get(match[1]) ?? [];
    const line = source.slice(0, match.index).split('\n').length;
    usages.push(`${path.relative(process.cwd(), file)}:${line}`);
    customPropertyUsages.set(match[1], usages);
  }
}

const runtimeCustomProperties = new Set(['--trigger-width']);
const undefinedCustomProperties = Array.from(customPropertyUsages.entries())
  .filter(([name]) => !customPropertyDefinitions.has(name) && !runtimeCustomProperties.has(name))
  .sort(([left], [right]) => left.localeCompare(right));

const duplicates = Array.from(occurrences.entries())
  .filter(([, locations]) => locations.length > 1)
  .map(([key, locations]) => {
    const [context, selector] = key.split('\u0000');
    return { context, selector, locations };
  })
  .sort((left, right) => left.selector.localeCompare(right.selector));

if (duplicates.length > 0) {
  console.error(`Duplicate selectors in the same cascade context: ${duplicates.length}`);
  for (const duplicate of duplicates) {
    const locations = duplicate.locations
      .map(({ file, line }) => `${file}:${line}`)
      .join(', ');
    console.error(`  ${duplicate.selector} [${duplicate.context}] — ${locations}`);
  }
}

if (forbiddenGlobals.length > 0) {
  console.error(`Feature/dashboard selectors remaining in globals.css: ${forbiddenGlobals.length}`);
  for (const entry of forbiddenGlobals) {
    console.error(`  ${entry.selector} — ${entry.file}:${entry.line}`);
  }
}

if (undersizedText.length > 0) {
  console.error(`Text declarations below the 10px floor: ${undersizedText.length}`);
  for (const entry of undersizedText) {
    console.error(`  ${entry.pixels}px — ${entry.file}:${entry.line}`);
  }
}

if (undefinedCustomProperties.length > 0) {
  console.error(`Undefined CSS custom properties without fallbacks: ${undefinedCustomProperties.length}`);
  for (const [name, usages] of undefinedCustomProperties) {
    console.error(`  ${name} — ${usages.join(', ')}`);
  }
}

if (
  duplicates.length > 0
  || forbiddenGlobals.length > 0
  || undersizedText.length > 0
  || undefinedCustomProperties.length > 0
) process.exit(1);
console.log(`CSS structure OK: ${files.length} files, zero duplicate selectors, no feature selectors in globals.css, no text below 10px, and no undefined custom properties.`);
