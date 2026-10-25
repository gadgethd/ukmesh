export type AutoRebaselineDecision = {
  action: 'skip' | 'rebaseline' | 'abort';
  reason: string;
  mismatches: string[];
};

type RefreshValidationSummary = {
  baselineOk?: unknown;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function abort(reason: string, mismatches: string[] = []): AutoRebaselineDecision {
  return { action: 'abort', reason, mismatches };
}

/** Decide whether /readyz describes a settled owner ACL generation drift. */
export function decideOwnerBaselineAction(
  httpStatus: number,
  payload: unknown,
): AutoRebaselineDecision {
  if (httpStatus !== 200 && httpStatus !== 503) {
    return abort(`unexpected-readyz-http-status-${httpStatus}`);
  }
  if (!isRecord(payload) || !isRecord(payload['checks'])) {
    return abort('readyz-payload-invalid');
  }

  const ownerAuthorization = payload['checks']['ownerAuthorization'];
  if (!isRecord(ownerAuthorization) || !isRecord(ownerAuthorization['inventoryBaseline'])) {
    return abort('owner-baseline-readiness-missing');
  }
  const baseline = ownerAuthorization['inventoryBaseline'];
  if (baseline['required'] !== true) {
    return { action: 'skip', reason: 'owner-baseline-not-required', mismatches: [] };
  }
  if (typeof baseline['ok'] !== 'boolean' || !Array.isArray(baseline['mismatches'])
    || baseline['mismatches'].some((value) => typeof value !== 'string')) {
    return abort('owner-baseline-readiness-invalid');
  }

  const mismatches = baseline['mismatches'] as string[];
  if (baseline['ok'] === true) {
    return mismatches.length === 0
      ? { action: 'skip', reason: 'owner-baseline-current', mismatches }
      : abort('owner-baseline-readiness-inconsistent', mismatches);
  }
  if (mismatches.length === 0) return abort('owner-baseline-failed-without-mismatch');
  if (httpStatus !== 503) return abort('owner-baseline-drift-without-degraded-readiness', mismatches);
  if (!mismatches.includes('aclDesiredGeneration')) {
    return abort('mismatch-is-not-owner-acl-generation-drift', mismatches);
  }
  const ownerAclMismatchFields = new Set([
    'aclDesiredGeneration',
    'aclRenderedGeneration',
    'aclAppliedGeneration',
    'aclReadback',
  ]);
  if (mismatches.some((mismatch) => !ownerAclMismatchFields.has(mismatch))) {
    return abort('unexpected-owner-baseline-mismatch', mismatches);
  }
  if (ownerAuthorization['aclMode'] !== 'apply') {
    return abort('owner-acl-mode-is-not-apply', mismatches);
  }

  const desired = ownerAuthorization['desiredGeneration'];
  const rendered = ownerAuthorization['renderedGeneration'];
  const applied = ownerAuthorization['appliedGeneration'];
  if (typeof desired !== 'string' || desired.length === 0
    || rendered !== desired || applied !== desired
    || ownerAuthorization['lastError'] !== null) {
    return abort('owner-acl-generation-is-not-settled', mismatches);
  }

  return { action: 'rebaseline', reason: 'settled-owner-acl-generation-drift', mismatches };
}

/** Extract the machine-readable summary emitted by refresh-owner-baseline.sh. */
export function parseRefreshValidationSummary(
  stdout: string,
): RefreshValidationSummary | null {
  const marker = '\n{\n  "exportPath":';
  const markerIndex = stdout.lastIndexOf(marker);
  if (markerIndex < 0) return null;

  const start = markerIndex + 1;
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = start; index < stdout.length; index += 1) {
    const character = stdout[index];
    if (inString) {
      if (escaped) escaped = false;
      else if (character === '\\') escaped = true;
      else if (character === '"') inString = false;
      continue;
    }
    if (character === '"') inString = true;
    else if (character === '{') depth += 1;
    else if (character === '}') {
      depth -= 1;
      if (depth === 0) {
        try {
          const parsed: unknown = JSON.parse(stdout.slice(start, index + 1));
          return isRecord(parsed) && 'baselineOk' in parsed
            ? parsed as RefreshValidationSummary
            : null;
        } catch {
          return null;
        }
      }
    }
  }
  return null;
}

/** The refresh script must both exit successfully and report the strict boolean true. */
export function refreshValidationPassed(
  exitCode: number | null,
  summary: RefreshValidationSummary | null,
): boolean {
  return exitCode === 0 && summary?.baselineOk === true;
}
