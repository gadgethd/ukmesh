// A finite workload process in the backend's network namespace. No credentials.
import assert from 'node:assert/strict';
import http from 'node:http';

const agents = Array.from({length: 8}, (_, index) => new http.Agent({keepAlive: true, maxSockets: 1,
  localAddress: '127.0.4.' + (index + 1)}));
const counts = agents.map(() => ({completed: 0, failed: 0}));
let stop = false, independentIdentities = false, failure;
process.on('SIGTERM', () => {stop = true;});
process.on('SIGINT', () => {stop = true;});
setTimeout(() => {stop = true;}, 480000).unref();
async function request(index) {
  return new Promise((resolve, reject) => {
    const call = http.get('http://127.0.0.1:3000/api/nodes/map?network=ukmesh&q=qual', {agent: agents[index]}, response => {
      let body = '';
      response.on('data', value => {body += value; if (body.length > 1048576) call.destroy(new Error('response budget exceeded'));});
      response.on('error', reject);
      response.on('end', () => {try {
        assert.equal(response.statusCode, 200); JSON.parse(body);
        const remaining = Number(response.headers['ratelimit-remaining']);
        assert.ok(Number.isSafeInteger(remaining) && remaining >= 0, 'finite limiter evidence is required');
        counts[index].completed++;
        resolve(remaining);
      } catch (error) {reject(error);}});
    });
    call.setTimeout(10000, () => call.destroy(new Error('lookup deadline exceeded')));
    call.on('error', reject);
  });
}
try {
  const before = await request(0);
  await request(1);
  const after = await request(0);
  assert.equal(after, before - 1, 'the second client must have a separate limiter identity');
  independentIdentities = true;
  await Promise.all(agents.map((_agent, index) => request(index)));
  console.log(JSON.stringify({ready: true, independentIdentities: true, clients: 8}));
  await Promise.all(agents.map(async (_agent, index) => {
    while (!stop) {
      try {await request(index);} catch {counts[index].failed++;}
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }));
} catch (error) {failure = error.message; process.exitCode = 1;}
finally {
  agents.forEach(agent => agent.destroy());
  const passed = independentIdentities && counts.every(value => value.completed > 0 && value.failed === 0);
  console.log(JSON.stringify({passed, independentIdentities, clients: counts, error: failure}));
  if (!passed) process.exitCode = 1;
}
