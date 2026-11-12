import { parseSync } from 'vite';

/** Keep each distinct legal notice in every chunk that contains it. Use parser
 * positions so comment-like text in strings, templates and regexes is untouched.
 */
export function deduplicateLegalComments(code: string, filename: string): string | null {
  const parsed = parseSync(filename, code);
  if (parsed.errors.length) throw new Error(`Cannot inspect legal notices in ${filename}`);
  const seen = new Set<string>();
  const pieces: string[] = [];
  let cursor = 0;
  for (const comment of parsed.comments) {
    if (!/^!|@license|@preserve/.test(comment.value)) continue;
    const notice = code.slice(comment.start, comment.end);
    if (seen.has(notice)) {
      // Preserve line-terminator semantics (for example, `return /*…*/ x`).
      const separator = /[\r\n\u2028\u2029]/.test(notice) ? '\n' : ' ';
      pieces.push(code.slice(cursor, comment.start), separator);
      cursor = comment.end;
    } else {
      seen.add(notice);
    }
  }
  if (!cursor) return null;
  pieces.push(code.slice(cursor));
  return pieces.join('');
}
