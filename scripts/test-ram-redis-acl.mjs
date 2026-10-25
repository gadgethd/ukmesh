#!/usr/bin/env node
// Own one bounded Redis instance and four principals for the RAM operational
// gates. Credentials stay in memory/hashed ACL files and never enter argv.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomBytes,createHash} from 'node:crypto';
import {spawn,spawnSync} from 'node:child_process';
import {Redis} from '../backend/node_modules/ioredis/built/index.js';

const backend=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../backend');
const database=process.env.TEST_INGEST_DATABASE_URL;
if(!database||!['127.0.0.1','localhost'].includes(new URL(database).hostname)) {
  throw new Error('TEST_INGEST_DATABASE_URL must be an owned loopback fixture; skipped runtime gates cannot pass');
}
const root=process.env.TEST_RAM_ACL_FIXTURE_ROOT??os.tmpdir();
fs.mkdirSync(root,{recursive:true});
const fixture=fs.mkdtempSync(path.join(root,'ram-acl-'));
fs.chmodSync(fixture,0o700);
fs.mkdirSync(path.join(fixture,'data'),{mode:0o700});
const name=`ram-acl-test-${process.pid}-${Date.now()}`;
const image=process.env.TEST_RAM_ACL_IMAGE??'redis@sha256:8b81dd37ff027bec4e516d41acfbe9fe2460070dc6d4a4570a2ac5b9d59df065';
const passwords=Object.fromEntries(['admin','writer','public','operator'].map(user=>[user,randomBytes(32).toString('hex')]));
const auth=user=>'#'+createHash('sha256').update(passwords[user]).digest('hex');
const acl=`user default off
user admin on ${auth('admin')} ~* &* +@all
user writer on ${auth('writer')} ~meshcore:acl:* &meshcore:live +@connection +@stream +@string +@hash +del +eval +evalsha +info +memory
user public on ${auth('public')} +ping
user operator on ${auth('operator')} ~meshcore:acl:*:quarantine +@connection +xrange +xlen +del +memory
`;
fs.writeFileSync(path.join(fixture,'users.acl'),acl,{mode:0o600});
const docker=(args)=>{
  const result=spawnSync('docker',args,{encoding:'utf8'});
  if(result.status!==0)throw new Error(`Owned Docker fixture failed: ${result.stderr.trim()}`);
  return result.stdout.trim();
};
let created=false;
try{
  const user=os.userInfo();
  docker(['run','-d','--rm','--name',name,'--memory','80m','--cpus','1','--user',`${user.uid}:${user.gid}`,
    '-p','127.0.0.1::6379','-v',`${fixture}:/fixture:ro`,'-v',`${fixture}/data:/data`,
    image,'redis-server','--dir','/data','--maxmemory','48mb','--maxmemory-policy','noeviction',
    '--appendonly','yes','--appendfsync','everysec','--aclfile','/fixture/users.acl']);
  created=true;
  const port=docker(['inspect',name,'--format','{{(index (index .NetworkSettings.Ports "6379/tcp") 0).HostPort}}']);
  const url=user=>`redis://${user}:${passwords[user]}@127.0.0.1:${port}/0`;
  const admin=new Redis(url('admin'),{enableReadyCheck:false,maxRetriesPerRequest:1});
  admin.on('error',()=>undefined);
  await admin.ping();await admin.quit();
  const env={...process.env,TEST_REDIS_ACL_URL:url('writer'),TEST_REDIS_ADMIN_URL:url('admin'),
    TEST_REDIS_PUBLIC_URL:url('public'),TEST_REDIS_OPERATOR_URL:url('operator'),
    TEST_REDIS_URL:url('admin'),TEST_REDIS_RUNTIME_URL:url('writer')};
  console.log('Owned ACL fixture: Redis maxmemory=48MiB, container limit=80MiB, noeviction, AOF, four isolated principals');
  for(const [file,expected] of [['src/ram/operational.integration.test.ts',4],['src/ram/runtime.integration.test.ts',1]]){
    let transcript='';
    const code=await new Promise((resolve,reject)=>{
      const child=spawn(process.execPath,['--import','tsx','--test',file],{cwd:backend,env,stdio:['ignore','pipe','pipe']});
      child.stdout.on('data',chunk=>{transcript+=chunk;process.stdout.write(chunk);});
      child.stderr.pipe(process.stderr);
      child.once('error',reject);child.once('exit',(code,signal)=>resolve(signal?1:code));
    });
    const complete=code===0&&new RegExp(`^# tests ${expected}$`,'m').test(transcript)
      &&new RegExp(`^# pass ${expected}$`,'m').test(transcript)
      &&/^# skipped 0$/m.test(transcript)&&/^# fail 0$/m.test(transcript);
    if(!complete)throw new Error(`Operational gate failed or incomplete: ${file}`);
  }
}finally{
  if(created)docker(['stop','--time','2',name]);
  fs.rmSync(fixture,{recursive:true,force:true});
}
