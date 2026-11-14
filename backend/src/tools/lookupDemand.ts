import { readFile } from 'node:fs/promises';
import type { LookupHttpCase, LookupHttpResult } from './lookupHttpRequests.js';
import { LOOKUP_RESPONSE_VALIDATION_LIMITS } from './lookupResponseVerifier.js';

export type DemandPolicy = { rps: number; duration_seconds: number; requests: number; clients: number;
  max_active: number; cache: 'warm'; warmups_per_case: number; max_admission_lateness_ms: number;
  min_dispatch_interval_ms: number; dispatch_span_tolerance_ms: number; response_deadline_ms: number;
  collection_budget_seconds: number; case_order: string[] };

export async function readDemandPolicy(file: string): Promise<DemandPolicy> {
  const value = JSON.parse(await readFile(file, 'utf8'));
  const p = value.demand as DemandPolicy;
  if (value.version !== 4 || !p || p.cache !== 'warm' || p.clients !== 128
    || p.max_active !== LOOKUP_RESPONSE_VALIDATION_LIMITS.maxPending || p.response_deadline_ms !== 30_000
    || !Array.isArray(p.case_order) || p.case_order.length !== 12 || new Set(p.case_order).size !== 12
    || p.case_order.some(name => typeof name !== 'string' || !name)
    || ['rps','duration_seconds','requests','warmups_per_case','max_admission_lateness_ms','min_dispatch_interval_ms',
      'dispatch_span_tolerance_ms','collection_budget_seconds'].some(key =>
        !Number.isFinite(p[key as keyof DemandPolicy]) || Number(p[key as keyof DemandPolicy]) <= 0)
    || !Number.isSafeInteger(p.requests) || p.requests !== p.rps * p.duration_seconds
    || p.min_dispatch_interval_ms > 1000 / p.rps) throw new Error('invalid shared matched-demand policy');
  return p;
}

type Started = { timing: { startedAtMonoMs: number; socketAtMonoMs: number | null;
  connectedAtMonoMs: number | null; receivedAtMonoMs?: number | null; responseStatus?: number | null;
  responseBytes?: number; actor: string }; assigned?: Promise<number>; done: Promise<LookupHttpResult> };
export type DemandSample = { index: number; name: string; actor: string; scheduledMs: number;
  startMs: number | null; socketMs: number | null; connectedMs: number | null; endMs: number | null;
  completedMs: number | null; latenessMs: number | null; active: number | null; queue: number | null;
  attemptMs: number | null;
  ms: number | null; status: number | null; passed: boolean; bytes: number | null; entries: number | null;
  digest: string | null; dataDigest: string | null; error?: string };
type Clock = { now: () => number; pause: (ms: number) => Promise<void> };
const realClock: Clock = { now: () => performance.now(), pause: ms => new Promise(resolve => setTimeout(resolve, ms)) };

/** Fixed input, bounded admission and full planned ledger. Never retry or erase late work. */
export async function runMatchedDemand(policy: DemandPolicy, definitions: LookupHttpCase[],
  originMs: number, startAtMs: number, start: (definition: LookupHttpCase, actor: number) => Started,
  clock: Clock = realClock) {
  if (JSON.stringify(definitions.map(row => row.name)) !== JSON.stringify(policy.case_order)) {
    throw new Error('selected source does not expose the exact shared twelve-case order');
  }
  const interval = 1000 / policy.rps, finish = startAtMs + policy.duration_seconds * 1000;
  const rows: DemandSample[] = Array.from({ length: policy.requests }, (_, index) => ({ index,
    name: definitions[index % definitions.length]!.name, actor: `127.0.1.${index % policy.clients + 1}`,
    scheduledMs: startAtMs-originMs+index*interval, startMs:null,socketMs:null,connectedMs:null,endMs:null,
    completedMs:null,latenessMs:null,active:null,queue:null,attemptMs:null,ms:null,status:null,passed:false,bytes:null,
    entries:null,digest:null,dataDigest:null }));
  const pending = new Set<Promise<void>>();
  const errors: string[] = []; let maxActive = 0, maxQueue = 0, last = -Infinity;
  let previous = clock.now();
  const now = () => { const value = clock.now(); if (!Number.isFinite(value) || value < previous) throw new Error('matched-demand clock moved backwards'); previous=value; return value; };
  const until = async (target: number) => { while (now() < target) await clock.pause(target-now()); };
  try {
    for (const row of rows) {
      await until(Math.max(originMs+row.scheduledMs,last+policy.min_dispatch_interval_ms));
      while (pending.size >= policy.max_active) await Promise.race(pending);
      await until(Math.max(originMs+row.scheduledMs,last+policy.min_dispatch_interval_ms));
      if (now() > finish+policy.dispatch_span_tolerance_ms) break;
      const dueBehind = Math.max(0, Math.min(policy.requests-1, Math.floor((now()-startAtMs)/interval))-row.index);
      row.queue = dueBehind; maxQueue=Math.max(maxQueue,dueBehind);
      row.active=pending.size+1; maxActive=Math.max(maxActive,row.active);
      row.attemptMs=now()-originMs;
      let request: Started;
      try { request=start(definitions[row.index % definitions.length]!,row.index % policy.clients); }
      catch (error) { row.error=error instanceof Error?error.message:String(error); errors.push(row.error); continue; }
      last=request.timing.startedAtMonoMs; row.startMs=last-originMs;
      let task: Promise<void>;
      task=request.done.then(result => {
        row.endMs=result.receivedAtMonoMs-originMs; row.ms=result.ms; row.status=result.status;
        row.bytes=result.bytes; row.entries=result.entries; row.digest=result.digest; row.dataDigest=result.dataDigest;
        const definition=definitions[row.index % definitions.length]!;
        if(!Number.isFinite(result.ms)||result.ms<0||result.ms>policy.response_deadline_ms
          || result.status!==(definition.expected??200)||!Number.isSafeInteger(result.bytes)
          ||result.bytes<0||result.bytes>LOOKUP_RESPONSE_VALIDATION_LIMITS.maxBytes
          ||(result.entries!==null&&(!Number.isSafeInteger(result.entries)||result.entries<0))
          ||(definition.populated&&result.entries===0)||!/^[a-f0-9]{64}$/.test(result.digest)
          ||!/^[a-f0-9]{64}$/.test(result.dataDigest))throw new Error('matched-demand response evidence failed');
        row.passed=true;
      }).catch(error => { row.error=error instanceof Error?error.message:String(error); }).finally(() => {
        row.socketMs=request.timing.socketAtMonoMs===null?null:request.timing.socketAtMonoMs-originMs;
        row.connectedMs=request.timing.connectedAtMonoMs===null?null:request.timing.connectedAtMonoMs-originMs;
        row.latenessMs=row.socketMs===null?null:row.socketMs-row.scheduledMs;
        if (row.endMs===null && request.timing.receivedAtMonoMs!=null) {
          row.endMs=request.timing.receivedAtMonoMs-originMs;row.ms=row.endMs-row.startMs!;
        }
        row.status??=request.timing.responseStatus??null; row.bytes??=request.timing.responseBytes??null;
        try { row.completedMs=now()-originMs; } catch(error) { errors.push(String(error)); }
        finally { pending.delete(task); }
      });
      pending.add(task);
      // Agent maxSockets can queue after the HTTP timer starts. Pace from the
      // real assignment, not the optimistic token/attempt, without awaiting body
      // or validation completion. An over100ms assignment remains a failed cohort.
      try {
        last=await (request.assigned ?? (request.timing.socketAtMonoMs===null
          ?Promise.reject(new Error('missing actual socket admission')):Promise.resolve(request.timing.socketAtMonoMs)));
      }catch(error){errors.push(String(error));}
    }
  } catch (error) { errors.push(error instanceof Error?error.message:String(error)); }
  finally { await Promise.allSettled([...pending]); }
  // The observation interval is exactly the fixed schedule, even if the final
  // fast response ends before its last12.5ms scheduling bucket closes.
  try { await until(finish); } catch (error) { errors.push(String(error)); }
  const admitted=rows.filter(row=>row.startMs!==null), completed=rows.filter(row=>row.completedMs!==null);
  const sockets=admitted.filter(row=>row.socketMs!==null).map(row=>row.socketMs!);
  const span=sockets.length ? sockets.at(-1)!-sockets[0]! : null;
  const maximumLate=admitted.length && admitted.every(row=>row.latenessMs!==null)
    ?Math.max(...admitted.map(row=>row.latenessMs!)):null;
  const intervalViolation=sockets.some((at,index)=>index>0 && at-sockets[index-1]! < policy.min_dispatch_interval_ms);
  const admission = { planned:rows.length,attempted:rows.filter(row=>row.attemptMs!==null).length,
    admitted:admitted.length,completed:completed.length,
    errors:rows.filter(row=>row.startMs!==null&&!row.passed).length,undelivered:rows.length-admitted.length,
    maxActive,maxQueue,maxLatenessMs:maximumLate,dispatchSpanMs:span };
  const plannedSpan=(policy.requests-1)*interval;
  const passed=errors.length===0 && admission.admitted===policy.requests && admission.completed===policy.requests
    && admission.errors===0 && sockets.length===policy.requests && maximumLate!==null && maximumLate<=policy.max_admission_lateness_ms
    && admitted.every(row=>row.latenessMs!==null&&row.latenessMs>=0 && row.connectedMs!==null)
    && !intervalViolation && span!==null && Math.abs(span-plannedSpan)<=policy.dispatch_span_tolerance_ms;
  return { passed, samples:rows, admission, window:{startMs:startAtMs-originMs,
      endMs:startAtMs-originMs+policy.duration_seconds*1000},
    errors, intervalViolation };
}
