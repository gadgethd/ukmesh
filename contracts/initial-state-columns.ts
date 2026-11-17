/** Lossless bootstrap transport. Database and privacy policy belong to the producer. */
export const INITIAL_STATE_COLUMNS_FORMAT = 'columns-v1';
export const INITIAL_STATE_FORMAT_PARAMETER = 'initial_state_format';
const collections = ['nodes', 'packets', 'messages', 'viable_links'] as const;
type Collection = typeof collections[number];
type Row = Record<string, unknown>;
export type InitialState = {
  type: 'initial_state';
  data: Record<Collection, Row[]>;
  ts: number;
};
type ColumnRows = { shapes: string[][]; rows: unknown[][] };
type ColumnState = { type: 'initial_state'; format: typeof INITIAL_STATE_COLUMNS_FORMAT;
  data: Record<Collection, ColumnRows>; ts: number };

/** Trusted producer only: own-data records, native Dates and parsed JSON values.
 * PostgreSQL DTOs have no accessors or custom JSON hooks. Keep references until
 * the final wire stringify, which owns Date, nested JSON and nonfinite semantics.
 * Omitted object fields must not turn into positional array nulls. */
export function encodeInitialStateColumns(value: InitialState): ColumnState {
  const data = Object.fromEntries(collections.map(name => {
    const shapes: string[][] = [], rows: unknown[][] = [];
    const indices = new Map<string, number>();
    for (const row of value.data[name]) {
      const shape = Object.keys(row).filter(key => {
        const field = row[key];
        return field !== undefined && typeof field !== 'function' && typeof field !== 'symbol';
      });
      const signature = JSON.stringify(shape);
      let index = indices.get(signature);
      if (index === undefined) { index = shapes.length; indices.set(signature, index); shapes.push(shape); }
      rows.push([index, ...shape.map(key => row[key])]);
    }
    return [name, { shapes, rows }];
  })) as Record<Collection, ColumnRows>;
  return { type: 'initial_state', format: INITIAL_STATE_COLUMNS_FORMAT, data, ts: value.ts };
}

function invalid(): never { throw new TypeError('Invalid initial-state column payload'); }
function object(value: unknown): value is Row {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && [Object.prototype, null].includes(Object.getPrototypeOf(value));
}
function dense(value: unknown): value is unknown[] {
  return Array.isArray(value) && Object.keys(value).length === value.length
    && Object.keys(value).every((key, index) => key === String(index));
}
function keys(value: unknown, expected: readonly string[]): value is Row {
  return object(value) && Object.keys(value).length === expected.length
    && expected.every(key => Object.prototype.hasOwnProperty.call(value, key));
}

/** Parsed producer wire only: JSON.parse owns nested JSON values. Validate the
 * column structure here; assemble own properties so __proto__ remains data. */
export function decodeInitialStateColumns(value: unknown): InitialState {
  if (!keys(value, ['type', 'format', 'data', 'ts']) || value['type'] !== 'initial_state'
    || value['format'] !== INITIAL_STATE_COLUMNS_FORMAT || typeof value['ts'] !== 'number'
    || !Number.isSafeInteger(value['ts']) || value['ts'] < 0 || !keys(value['data'], collections)) invalid();
  const source = value['data'];
  const data = Object.fromEntries(collections.map(name => {
    const collection = source[name];
    if (!keys(collection, ['shapes', 'rows']) || !dense(collection['shapes']) || !dense(collection['rows'])) invalid();
    const shapes: string[][] = [], signatures = new Set<string>();
    for (const shape of collection['shapes']) {
      if (!dense(shape) || !shape.every((key): key is string => typeof key === 'string')
        || new Set(shape).size !== shape.length) invalid();
      const signature = JSON.stringify(shape);
      if (signatures.has(signature)) invalid();
      signatures.add(signature); shapes.push(shape);
    }
    const used = new Set<number>();
    const rows = collection['rows'].map(row => {
      if (!dense(row) || typeof row[0] !== 'number' || !Number.isSafeInteger(row[0])
        || row[0] < 0 || row[0] >= shapes.length) invalid();
      const shape = shapes[row[0]]!;
      if (row.length !== shape.length + 1) invalid();
      used.add(row[0]);
      return Object.fromEntries(shape.map((key, index) => [key, row[index + 1]]));
    });
    if (used.size !== shapes.length) invalid();
    return [name, rows];
  })) as Record<Collection, Row[]>;
  return { type: 'initial_state', data, ts: value['ts'] };
}

/** Legacy and live envelopes pass through; an advertised compact frame must decode. */
export function decodeInitialStateMessage(value: unknown): unknown {
  return object(value) && value['type'] === 'initial_state' && Object.prototype.hasOwnProperty.call(value, 'format')
    ? decodeInitialStateColumns(value) : value;
}
