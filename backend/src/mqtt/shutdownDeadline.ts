/** Every shutdown step consumes the same wall-clock budget. */
export async function waitForShutdownStep<T>(
  operation: Promise<T>,
  deadline: number,
  step: string,
): Promise<T> {
  const remainingMs = deadline - Date.now();
  if (remainingMs <= 0) throw new Error(`MQTT_DRAIN_TIMEOUT:${step}`);
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => reject(new Error(`MQTT_DRAIN_TIMEOUT:${step}`)), remainingMs);
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}
