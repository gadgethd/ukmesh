// Only a clean one-shot admission refusal uses this child result. A completed
// child still needs the parent's independent SQL publication certificate check.
export const REGION_SNAPSHOT_DEFERRED_EXIT_CODE = 75;
export const REGION_SNAPSHOT_TRY_ADMISSION_ARG = '--try-heavy-admission';
export type RegionSnapshotChildOutcome = 'completed' | 'deferred';
