/** Reuse only the enclosing operation's fresh initial read. Publication and
 * response checks must still call the authoritative reader themselves. */
export async function readInitialVisibilityGeneration(
  read: () => Promise<number>,
  initial?: number,
): Promise<number> {
  const generation = initial ?? await read();
  if (!Number.isSafeInteger(generation) || generation < 1) {
    throw new Error('PUBLIC_VISIBILITY_GENERATION_UNAVAILABLE');
  }
  return generation;
}
