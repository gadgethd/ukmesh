import http from 'node:http';

const counts = {tags: 0, alerts: 0, failures: 0, lastAlertProbe: '', alertProbes: []};
let failureStatus = 0;
let delayMs = 0;
const server = http.createServer((request, response) => {
  handle(request, response).catch(error => {
    if (!response.headersSent) response.writeHead(error instanceof SyntaxError ? 400 : 500);
    response.end();
  });
});
async function handle(request, response) {
  const url = new URL(request.url, 'http://local-test');
  if (url.pathname === '/healthz' || url.pathname === '/counts') {
    response.writeHead(200, {'content-type': 'application/json'});
    response.end(JSON.stringify(counts));
    return;
  }
  if (request.method !== 'POST') { response.writeHead(405); response.end(); return; }
  let body = '';
  for await (const chunk of request) {
    body += chunk;
    if (Buffer.byteLength(body) > 1_048_576) { response.writeHead(413); response.end(); return; }
  }
  if (url.pathname === '/failure') {
    const value = JSON.parse(body);
    if (![0, 429, 503].includes(value.status) || !Number.isInteger(value.delayMs ?? 0)
      || (value.delayMs ?? 0) < 0 || (value.delayMs ?? 0) > 2000) { response.writeHead(400); response.end(); return; }
    failureStatus = value.status; delayMs = value.delayMs ?? 0;
    response.writeHead(204); response.end(); return;
  }
  if (delayMs) await new Promise(resolve => setTimeout(resolve, delayMs));
  if (failureStatus) { counts.failures++; response.writeHead(failureStatus); response.end(); return; }
  if (url.pathname === '/alerts') {
    const probe = body.match(/OwnedFullStackProbe-([a-f0-9]{32})/);
    if (probe) {
      counts.lastAlertProbe = probe[1];
      if (!counts.alertProbes.includes(probe[1])) counts.alertProbes.push(probe[1]);
      if (counts.alertProbes.length > 64) counts.alertProbes.shift();
    }
    counts.alerts++; response.writeHead(204); response.end(); return;
  }
  if (url.pathname === '/tag') {
    counts.tags++;
    response.writeHead(200, {'content-type': 'application/json'});
    response.end(JSON.stringify({answers: {speaker: {choice: 'human', confidence: 0.99}, kind: {choice: 'test-ping', confidence: 0.99}, directed: {noul: false, confidence: 0.99}, safety: {noul: false, confidence: 0.99}}}));
    return;
  }
  response.writeHead(404); response.end();
}
const port = Number(process.env.PORT ?? 8080);
if (!Number.isSafeInteger(port) || port < 0 || port > 65535) throw new Error('Invalid test endpoint port');
server.listen(port, '0.0.0.0', () => console.log(JSON.stringify({listening: server.address().port})));
process.on('SIGTERM', () => server.close());
