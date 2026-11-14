/** Fixed offered demand; selected routes and HTTP clocks share the native owner. */
import { mkdir,writeFile } from 'node:fs/promises';
import path from 'node:path';
import { option } from './sampleData.js';
import { configureLookupReadOnlyEnvironment,withLookupSource } from './lookupBenchmarkSession.js';
import { lookupSourceIdentity } from './lookupSource.js';
import { LookupTimeline } from './lookupTimeline.js';
import { loadLookupHttpWorkload } from './lookupHttpWorkload.js';
import { createLookupRequestFleet } from './lookupHttpRequests.js';
import { readDemandPolicy,runMatchedDemand } from './lookupDemand.js';
import { LOOKUP_RESPONSE_VALIDATION_LIMITS } from './lookupResponseVerifier.js';

const args=process.argv.slice(2), source=path.resolve(option(args,'source-root')??'src');
const output=path.resolve(option(args,'output')??'../plans/lean-runtime/qualification/matched-demand');
const policyFile=option(args,'policy');if(!policyFile)throw new Error('shared --policy required');
const policy=await readDemandPolicy(policyFile), timeline=new LookupTimeline();let completed=false;
const result: Record<string,unknown>={version:1,at:new Date().toISOString(),source,settings:policy,passed:false,
  responseValidationLimits:LOOKUP_RESPONSE_VALIDATION_LIMITS,warmup:[],samples:[],errors:[],timeline:'timeline.json'};
try {
  await timeline.span({phase:'benchmark'},async()=>{
    const identity=await lookupSourceIdentity(source,option(args,'reference-revision')??null);result.sourceIdentity=identity;
    configureLookupReadOnlyEnvironment(process.env['DATABASE_URL']??'',process.env);
    await withLookupSource(source,async(load,runtime)=>{
      const workload=await loadLookupHttpWorkload(load,runtime),fleet=createLookupRequestFleet();
      try {
        const prepared=await workload.prepare(option(args,'snapshot'));result.counts=prepared.counts;
        if(JSON.stringify(prepared.cases.map(row=>row.name))!==JSON.stringify(policy.case_order))throw new Error('shared twelve-case order differs');
        await workload.withTrial(async port=>{
          const warmup:Record<string,unknown>[]=[];result.warmup=warmup;
          await timeline.span({phase:'warmup'},async()=>{
            for(const definition of prepared.cases)for(let repeat=0;repeat<policy.warmups_per_case;repeat++){
              try { warmup.push({name:definition.name,repeat,passed:true,...await fleet.request(port,definition)}); }
              catch(error){warmup.push({name:definition.name,repeat,passed:false,error:String(error)});throw error;}
            }
          });
          const cpu=process.cpuUsage(),started=performance.now();
          await timeline.span({phase:'demand'},async boundary=>{
            // origin is the exact same UTC/monotonic bracket owner used by native
            // timelines. No assumed common epoch between Python and Node.
            const origin=timeline.originMonotonicMs;
            Object.assign(result,await runMatchedDemand(policy,prepared.cases,origin,boundary.clock.monotonicAfterMs,
              (definition,actor)=>fleet.start(port,definition,actor)));
          });
          result.resources={wallMs:performance.now()-started,cpuMicros:process.cpuUsage(cpu),memory:process.memoryUsage(),
            responseValidation:await fleet.readStatistics()};
        });
      }finally{await fleet.close();}
    });
    result.sourceIdentityAfter=await lookupSourceIdentity(source,identity.revision);
    result.sourceStable=JSON.stringify(result.sourceIdentityAfter)===JSON.stringify(identity);
    result.passed=result.passed===true&&result.sourceStable===true;
  });
  completed=result.passed===true;
}catch(error){(result.errors as string[]).push(error instanceof Error?error.message:String(error));result.passed=false;}
finally{
  if(result.sourceIdentity && !result.sourceIdentityAfter){
    try{
      result.sourceIdentityAfter=await lookupSourceIdentity(source,(result.sourceIdentity as {revision:string|null}).revision);
      result.sourceStable=JSON.stringify(result.sourceIdentityAfter)===JSON.stringify(result.sourceIdentity);
    }catch(error){(result.errors as string[]).push('source closure: '+String(error));result.sourceStable=false;}
  }
  if(!(result.samples as unknown[]).length){
    result.samples=Array.from({length:policy.requests},(_,index)=>({index,name:policy.case_order[index%policy.case_order.length],
      actor:`127.0.1.${index%policy.clients+1}`,scheduledMs:null,attemptMs:null,startMs:null,socketMs:null,connectedMs:null,
      endMs:null,completedMs:null,latenessMs:null,active:null,queue:null,ms:null,status:null,passed:false,bytes:null,
      entries:null,digest:null,dataDigest:null,error:'cohort did not reach admission'}));
    result.admission={planned:policy.requests,attempted:0,admitted:0,completed:0,errors:0,undelivered:policy.requests,
      maxActive:0,maxQueue:0,maxLatenessMs:null,dispatchSpanMs:null};
  }
  await mkdir(output,{recursive:true});
  await writeFile(path.join(output,'matched-demand.json'),JSON.stringify(result,null,2)+'\n',{flag:'wx'});
  await writeFile(path.join(output,'timeline.json'),JSON.stringify(timeline.finish(completed),null,2)+'\n',{flag:'wx'});
}
console.log(JSON.stringify({passed:result.passed,output,admission:result.admission}));
if(!completed)process.exitCode=1;
