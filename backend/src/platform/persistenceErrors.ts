/** Classification of persistence failures shared by every writer.
 *
 * SQLSTATE class 22 (data_exception) and class 23 (integrity violation) are
 * deterministic for a given row: retrying the identical insert cannot succeed.
 * Class 08 (connection) and class 40 (serialization/deadlock) stay retryable.
 */

const PERMANENT_SQLSTATE_PREFIXES = ['22', '23'] as const;

const PERMANENT_ERROR_MESSAGE =
  /invalid input syntax for type (?:smallint|integer|bigint|numeric)|out of range for type (?:smallint|integer|bigint)|violates (?:check|not-null|foreign key|unique|exclusion) constraint/i;

/** True when re-running the same insert cannot succeed (a data/constraint error). */
export function isPermanentPersistenceError(error: unknown): boolean {
  if (typeof error === 'object' && error !== null && 'code' in error) {
    const code = (error as { code?: unknown }).code;
    if (typeof code === 'string' && PERMANENT_SQLSTATE_PREFIXES.some((prefix) => code.startsWith(prefix))) {
      return true;
    }
  }
  const message = error instanceof Error
    ? error.message
    : (typeof error === 'string' ? error : '');
  return message !== '' && PERMANENT_ERROR_MESSAGE.test(message);
}
