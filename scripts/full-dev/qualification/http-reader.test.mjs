import assert from 'node:assert/strict';
import test from 'node:test';
import http from 'node:http';
import {once} from 'node:events';
import {mkdtemp,rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {allowedRoute,createReader} from './http-reader.mjs';

test('bounded read helper permits only existing measured GET routes', () => {
  for (const route of ['/api/nodes/map?network=ukmesh', '/api/packets/recent?limit=200',
    `/api/nodes/${'ab'.repeat(32)}/links`, `/api/packets/${'cd'.repeat(32)}`, '/api/stats/charts']) assert.ok(allowedRoute(route));
  for (const route of ['http://outside.invalid/api/nodes/map', '/api/owner/save', '/metrics', '/api/nodes/short/history']) assert.equal(allowedRoute(route), false);
});

test('native helper gives eight slots distinct real peers and survives upstream errors', {timeout:10000}, async () => {
  const peers=new Set();let active=0,maximum=0;
  const upstream=http.createServer((request,response)=>{
    peers.add(request.socket.remoteAddress);active++;maximum=Math.max(maximum,active);
    response.once('close',()=>active--);
    if(request.url.includes('fail=1')){request.socket.destroy();return;}
    setTimeout(()=>{response.writeHead(200);response.end('{"nodes":[]}');},30);
  });
  upstream.listen(0,'127.0.0.1');await once(upstream,'listening');
  const reader=createReader(upstream.address().port);
  const directory=await mkdtemp(path.join(os.tmpdir(),'owned-reader-'));const socket=path.join(directory,'reader.sock');
  reader.listen(socket);await once(reader,'listening');
  const get=(index,route='/api/nodes/map')=>new Promise((resolve,reject)=>{
    const request=http.get({socketPath:socket,path:route,headers:{'x-owned-reader-slot':String(index)}},response=>{
      response.resume();response.once('end',()=>resolve(response.statusCode));response.once('error',reject);
    });request.once('error',reject);
  });
  try{
    assert.deepEqual(await Promise.all(Array.from({length:8},(_,index)=>get(index))),Array(8).fill(200));
    assert.equal(peers.size,8);assert.ok(maximum<=8);
    assert.equal(await get(0,'/api/nodes/map?fail=1'),502);
    assert.equal(await get(0),200);
    assert.equal(await get(9),400);
  }finally{
    reader.close();reader.closeAllConnections();await once(reader,'close');
    upstream.close();upstream.closeAllConnections();await once(upstream,'close');await rm(directory,{recursive:true});
  }
});
