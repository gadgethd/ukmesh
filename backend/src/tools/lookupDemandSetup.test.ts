import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp,readFile,rm,writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import test from 'node:test';

test('setup failure retains all planned undelivered rows and failed real timeline without contacting a database',async t=>{
  const root=await mkdtemp(path.join(os.tmpdir(),'demand-setup-'));
  t.after(()=>rm(root,{recursive:true,force:true}));
  await writeFile(path.join(root,'owned.ts'),'export const owned=true;\n');
  const output=path.join(root,'output');
  const execute=()=>promisify(execFile)(process.execPath,['--max-old-space-size=128','--import',
    path.resolve('node_modules/tsx/dist/loader.mjs'),path.resolve('src/tools/benchmarkMatchedDemand.ts'),
    '--source-root',root,'--output',output,'--policy',path.resolve('../scripts/full-dev/qualification/read_protocol.json')],
    {env:{...process.env,DATABASE_URL:'postgresql://unused@not-local.invalid/fixture'},timeout:15000});
  await assert.rejects(execute(),error=>(error as {code:unknown}).code===1);
  const raw=await readFile(path.join(output,'matched-demand.json'),'utf8');const value=JSON.parse(raw);
  assert.equal(value.passed,false);assert.equal(value.samples.length,9600);
  assert.equal(value.admission.planned,9600);assert.equal(value.admission.undelivered,9600);
  assert.equal(value.admission.attempted,0);assert.ok(value.errors.some((error:string)=>/local|loopback/.test(error)));
  assert.ok(value.samples.every((row:{startMs:unknown;passed:boolean})=>row.startMs===null&&!row.passed));
  const timeline=JSON.parse(await readFile(path.join(output,'timeline.json'),'utf8'));
  assert.equal(timeline.passed,false);assert.equal(timeline.events.at(-1).outcome,'failed');
  await assert.rejects(execute());
  assert.equal(await readFile(path.join(output,'matched-demand.json'),'utf8'),raw,'failed rerun must preserve original evidence');
});
