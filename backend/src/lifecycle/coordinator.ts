export type LifecycleResource = {
  name: string;
  stage: number;
  close: () => void | Promise<void>;
};

export class LifecycleDeadlineError extends Error {
  constructor(
    readonly deadlineMs: number,
    readonly outstanding: string[],
  ) {
    super(
      `shutdown deadline exceeded after ${deadlineMs}ms; outstanding=${outstanding.join(',') || 'none'}`,
    );
  }
}

export class LifecycleCoordinator {
  private resources: LifecycleResource[] = [];
  private names = new Set<string>();
  private active = new Set<string>();
  private drainPromise: Promise<void> | null = null;
  private drainReason: string | null = null;

  constructor(private readonly deadlineMs = 30_000) {}

  get isDraining(): boolean {
    return this.drainPromise !== null;
  }

  register(resource: LifecycleResource): void {
    if (this.isDraining) throw new Error(`cannot register ${resource.name} while draining`);
    if (this.names.has(resource.name)) throw new Error(`duplicate lifecycle resource: ${resource.name}`);
    this.names.add(resource.name);
    this.resources.push(resource);
  }

  snapshot(): {
    draining: boolean;
    reason: string | null;
    outstanding: string[];
  } {
    return {
      draining: this.isDraining,
      reason: this.drainReason,
      outstanding: [...this.active].sort(),
    };
  }

  drain(reason: string): Promise<void> {
    if (this.drainPromise) return this.drainPromise;
    this.drainReason = reason;
    const work = async () => {
      const failures: Array<{ name: string; error: unknown }> = [];
      const stages = [...new Set(this.resources.map((resource) => resource.stage))]
        .sort((left, right) => left - right);
      for (const stage of stages) {
        const resources = this.resources.filter((resource) => resource.stage === stage);
        const results = await Promise.allSettled(resources.map(async (resource) => {
          this.active.add(resource.name);
          try {
            await resource.close();
          } finally {
            this.active.delete(resource.name);
          }
        }));
        for (let index = 0; index < results.length; index += 1) {
          const result = results[index];
          if (result?.status === 'rejected') {
            failures.push({
              name: resources[index]?.name ?? `stage-${stage}-${index}`,
              error: result.reason,
            });
          }
        }
      }
      if (failures.length > 0) {
        throw new AggregateError(
          failures.map((failure) => failure.error),
          `shutdown resources failed: ${failures.map((failure) => failure.name).join(', ')}`,
        );
      }
    };

    let deadlineTimer: NodeJS.Timeout | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      deadlineTimer = setTimeout(() => {
        reject(new LifecycleDeadlineError(
          this.deadlineMs,
          this.snapshot().outstanding,
        ));
      }, this.deadlineMs);
    });
    this.drainPromise = Promise.race([work(), deadline])
      .finally(() => {
        if (deadlineTimer) clearTimeout(deadlineTimer);
      });
    return this.drainPromise;
  }
}
