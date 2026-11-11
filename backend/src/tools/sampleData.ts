/** Local-only helpers for the UKMesh sample dataset.
 *
 * The dataset itself is never committed. These helpers only read a directory
 * supplied on the command line (or the local `data/` path) and never log row
 * payloads.
 */
import { readFile } from 'node:fs/promises';
import { gunzipSync } from 'node:zlib';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export type CsvTable = {
  headers: string[];
  rows: string[][];
};

/** RFC4180 parser for the sample files (quoted commas, quotes and newlines). */
export function parseCsv(text: string): CsvTable {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = '';
  let quoted = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index]!;
    if (quoted) {
      if (char === '"') {
        if (text[index + 1] === '"') {
          field += '"';
          index += 1;
        } else {
          quoted = false;
        }
      } else {
        field += char;
      }
      continue;
    }
    if (char === '"') {
      quoted = true;
    } else if (char === ',') {
      row.push(field);
      field = '';
    } else if (char === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
    } else if (char !== '\r') {
      field += char;
    }
  }
  if (field !== '' || row.length > 0) {
    row.push(field);
    rows.push(row);
  }
  if (rows.length > 0 && rows[rows.length - 1]!.length === 1 && rows[rows.length - 1]![0] === '') {
    rows.pop();
  }
  const [headers = [], ...data] = rows;
  return { headers, rows: data };
}

export async function readGzipCsv(file: string): Promise<CsvTable> {
  const compressed = await readFile(file);
  return parseCsv(gunzipSync(compressed).toString('utf8'));
}

export function rowObject(headers: readonly string[], row: readonly string[]): Record<string, string> {
  const result: Record<string, string> = {};
  headers.forEach((header, index) => {
    result[header] = row[index] ?? '';
  });
  return result;
}

export function defaultDataDir(): string {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../data/ukmesh-sample-2026-11-10');
}

export function option(args: readonly string[], name: string): string | undefined {
  const prefix = `--${name}=`;
  const inline = args.find((arg) => arg.startsWith(prefix));
  if (inline) return inline.slice(prefix.length);
  const index = args.indexOf(`--${name}`);
  if (index === -1) return undefined;
  const value = args[index + 1];
  return value && !value.startsWith('--') ? value : undefined;
}

export function flag(args: readonly string[], name: string): boolean {
  return args.includes(`--${name}`);
}

export function requiredOption(args: readonly string[], name: string): string {
  const value = option(args, name);
  if (!value) throw new Error(`--${name} is required`);
  return value;
}

export function parseIntOption(args: readonly string[], name: string, fallback: number): number {
  const raw = option(args, name);
  if (raw === undefined) return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) throw new Error(`--${name} must be a number`);
  return parsed;
}

export function parseTimeMs(value: string | undefined, name: string): number | undefined {
  if (!value) return undefined;
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) throw new Error(`--${name} must be an ISO timestamp`);
  return parsed;
}

export function csvNumber(value: string): number | null {
  if (value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function csvInteger(value: string): number | null {
  const parsed = csvNumber(value);
  return parsed === null ? null : Math.trunc(parsed);
}

export function csvBoolean(value: string): boolean | null {
  if (value === '') return null;
  if (value === 't' || value === 'true' || value === '1') return true;
  if (value === 'f' || value === 'false' || value === '0') return false;
  return null;
}

export function csvJson(value: string): unknown | null {
  if (value === '') return null;
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return null;
  }
}
