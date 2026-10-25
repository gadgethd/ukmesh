/** Join the active writer tick before starting shutdown drain. */
export async function joinAndDrainRamWriter(
  writer: { requestStop(): void; drain(deadlineMs: number): Promise<void> },
  loop: Promise<void>,
  deadline: number,
  now: () => number = Date.now,
): Promise<void> {
  const withinDeadline = async (work: Promise<void>, stage: string): Promise<void> => {
    const remaining = deadline - now();
    if (remaining <= 0) throw new Error(`RAM_DRAIN_TIMEOUT:${stage}`);
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        work,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error(`RAM_DRAIN_TIMEOUT:${stage}`)), remaining);
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  };
  writer.requestStop();
  await withinDeadline(loop, 'writer-loop');
  await withinDeadline(writer.drain(Math.max(0, deadline - now())), 'pending-drain');
}
