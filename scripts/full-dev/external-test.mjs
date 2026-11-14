import http from 'node:http';

const counts = {tags: 0, alerts: 0, failures: 0, lastAlertProbe: '', alertProbes: [], failedAlertProbes: []};
let fault = {status: 0, delayMs: 0, route: '/tag', alertProbe: null};
function remember(values, value) {
  if (!values.includes(value)) values.push(value);
  if (values.length > 64) values.shift();
}
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
    const route = value?.route ?? '/tag';
    const delayMs = value?.delayMs ?? 0;
    const alertProbe = value?.alertProbe ?? null;
    const validTagFault = route === '/tag' && [0, 429].includes(value?.status) && alertProbe === null;
    const validAlertFault = route === '/alerts' && value?.status === 503 && delayMs === 0
      && typeof alertProbe === 'string' && /^[a-f0-9]{32}$/.test(alertProbe);
    if ((!validTagFault && !validAlertFault) || !Number.isInteger(delayMs)
      || delayMs < 0 || delayMs > 2000) { response.writeHead(400); response.end(); return; }
    fault = {status: value.status, delayMs, route, alertProbe};
    response.writeHead(204); response.end(); return;
  }
  const probe = url.pathname === '/alerts' ? body.match(/(?<![A-Za-z0-9_-])OwnedFullStackProbe-([a-f0-9]{32})(?![A-Za-z0-9_-])/)?.[1] : null;
  const applies = url.pathname === fault.route && (fault.alertProbe === null || probe === fault.alertProbe);
  const currentFault = applies ? fault : {status: 0, delayMs: 0};
  if (currentFault.delayMs) await new Promise(resolve => setTimeout(resolve, currentFault.delayMs));
  if (currentFault.status) {
    counts.failures++;
    if (probe) remember(counts.failedAlertProbes, probe);
    response.writeHead(currentFault.status); response.end(); return;
  }
  if (url.pathname === '/alerts') {
    if (probe) {
      counts.lastAlertProbe = probe;
      remember(counts.alertProbes, probe);
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
