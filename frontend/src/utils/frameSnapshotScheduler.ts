export type FrameSnapshotSchedulerOptions = {
  emit: () => void;
  minIntervalMs: number;
  isVisible: () => boolean;
  requestFrame?: (callback: FrameRequestCallback) => number;
  cancelFrame?: (handle: number) => void;
};

export function createFrameSnapshotScheduler(options: FrameSnapshotSchedulerOptions) {
  const requestFrame = options.requestFrame ?? requestAnimationFrame;
  const cancelFrame = options.cancelFrame ?? cancelAnimationFrame;
  let frame: number | null = null;
  let stopped = false;
  let lastEmitAt = Number.NEGATIVE_INFINITY;

  const render = (timestamp: number) => {
    frame = null;
    if (stopped || !options.isVisible()) return;
    if (timestamp - lastEmitAt < options.minIntervalMs) {
      frame = requestFrame(render);
      return;
    }
    lastEmitAt = timestamp;
    options.emit();
  };

  return {
    noteMutation: () => {
      if (stopped || frame !== null || !options.isVisible()) return;
      frame = requestFrame(render);
    },
    stop: () => {
      stopped = true;
      if (frame !== null) cancelFrame(frame);
      frame = null;
    },
    snapshot: () => ({ scheduled: frame !== null, lastEmitAt }),
  };
}
