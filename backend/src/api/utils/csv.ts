export type CsvScalar = string | number | boolean | null | undefined;
const LEADING_CONTROL = /^[\u0000-\u001f]/u;
const FORMULA_AFTER_PREFIX = /^[\u0000-\u0020\uFEFF]*[=+\-@]/u;

export function spreadsheetSafeText(value: string): string {
  if (value.startsWith("'")) return value;
  return LEADING_CONTROL.test(value) || FORMULA_AFTER_PREFIX.test(value)
    ? `'${value}`
    : value;
}

export function csvCell(value: CsvScalar): string {
  if (value == null) return '';
  const text = typeof value === 'string' ? spreadsheetSafeText(value) : String(value);
  return /[",\n\r]/u.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

export function csvRow(values: readonly CsvScalar[]): string {
  return values.map(csvCell).join(',');
}
