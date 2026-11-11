/** A caller-supplied value failed validation. The API error mapper turns this
 *  into a 4xx response; domain modules may throw it without importing HTTP. */
export class ApiInputError extends Error {
  constructor(
    message: string,
    readonly code = 'INVALID_INPUT',
    readonly status = 400,
  ) {
    super(message);
  }
}
