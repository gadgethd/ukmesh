/** Local-only helpers for the UKMesh sample dataset.
 *
 * The dataset itself is never committed. These helpers only read a directory
 * supplied on the command line (or the local `data/` path) and never log row
 * payloads.
 */
import { createGunzip } from 'node:zlib';
import { createReadStream } from 'node:fs';
import { StringDecoder } from 'node:string_decoder';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export type CsvTable = {
  headers: string[];
  rows: string[][];
};

/** Sample writes are limited to owned loopback fixtures. Reject host overrides
 * in connection parameters as well as remote authorities. */
export function assertLocalDatabaseUrl(value: string): void {
  const url = new URL(value);
  if (!['postgres:', 'postgresql:'].includes(url.protocol)
    || !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)
    || url.searchParams.has('host') || url.searchParams.has('hostaddr')
    || !url.pathname || url.pathname === '/') {
    throw new Error('sample tools require an explicit loopback PostgreSQL database');
  }
}

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
  let headers: string[] | undefined;
  const rows: string[][] = [];
  for await (const record of gzipCsvRecords(file)) {
    if (!headers) headers = record;
    else rows.push(record);
  }
  return { headers: headers ?? [], rows };
}

/** Stream complete RFC4180 records; quoted newlines and escaped quotes may
 * cross gzip/UTF-8 chunk boundaries. Retains at most one record. */
export async function* gzipCsvRecords(file: string): AsyncGenerator<string[]> {
  const source = createReadStream(file);
  const stream = source.pipe(createGunzip());
  source.on('error', (error) => stream.destroy(error));
  const decoder = new StringDecoder('utf8');
  let row: string[] = [];
  let field: string[] = [];
  let quoted = false;
  let closingQuote = false;
  function* consume(text: string): Generator<string[]> {
    for (const char of text) {
      if (quoted) {
        if (closingQuote) {
          closingQuote = false;
          if (char === '"') { field.push('"'); continue; }
          quoted = false;
        } else {
          if (char === '"') closingQuote = true;
          else field.push(char);
          continue;
        }
      }
      if (char === '"' && field.length === 0) quoted = true;
      else if (char === ',') { row.push(field.join('')); field = []; }
      else if (char === '\n') {
        row.push(field.join('')); field = [];
        yield row; row = [];
      } else if (char !== '\r') field.push(char);
    }
  }
  try {
    for await (const chunk of stream) yield* consume(decoder.write(chunk as Buffer));
    yield* consume(decoder.end());
    if (quoted && !closingQuote) throw new Error(`${path.basename(file)}: unterminated quoted CSV field`);
    if (field.length > 0 || row.length > 0) { row.push(field.join('')); yield row; }
  } finally { source.destroy(); stream.destroy(); }
}

export async function inspectGzipCsv(file: string, headersOnly = false): Promise<{
  headers: string[]; count: number; firstTime: number | null; lastTime: number | null;
}> {
  let headers: string[] | undefined;
  let count = 0;
  let firstTime: number | null = null;
  let lastTime: number | null = null;
  let timeIndex = -1;
  for await (const record of gzipCsvRecords(file)) {
    if (!headers) {
      headers = record;
      if (headersOnly) break;
      timeIndex = headers.indexOf('time');
    } else {
      if (record.length !== headers.length) throw new Error(`${path.basename(file)}: CSV width mismatch at row ${count + 2}`);
      count += 1;
      const time = Date.parse(record[timeIndex] ?? '');
      if (Number.isFinite(time)) {
        firstTime = Math.min(firstTime ?? time, time);
        lastTime = Math.max(lastTime ?? time, time);
      }
    }
  }
  return { headers: headers ?? [], count, firstTime, lastTime };
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
