#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

required_services=(
  mesh-health-check
  anubis-health-check
  cloudflared
)

for service in "${required_services[@]}"; do
  if ! docker compose ps --status running --services | grep -qx "$service"; then
    echo "FAIL: docker compose service is not running: $service" >&2
    exit 1
  fi
done

docker compose exec -T mesh-health-check node <<'NODE'
const http = require('node:http');

const checks = [
  {
    name: 'mesh-health-check direct API',
    host: 'mesh-health-check',
    port: 3090,
    path: '/api/bootstrap',
    headers: { Host: 'healthcheck.ukmesh.com' },
    expectStatus: 200,
    expectContentType: 'application/json',
  },
  {
    name: 'Cloudflare origin alias via Anubis',
    host: 'anubis-mesh-health-check',
    port: 8923,
    path: '/api/bootstrap',
    headers: {
      Host: 'healthcheck.ukmesh.com',
      'CF-Connecting-IP': '127.0.0.1',
    },
    expectStatus: 200,
    expectContentType: 'application/json',
  },
];

function request(check) {
  return new Promise((resolve, reject) => {
    const req = http.get({
      host: check.host,
      port: check.port,
      path: check.path,
      headers: check.headers,
      timeout: 3000,
    }, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        body += chunk;
      });
      res.on('end', () => {
        const contentType = String(res.headers['content-type'] || '');
        if (res.statusCode !== check.expectStatus) {
          reject(new Error(`${check.name}: expected HTTP ${check.expectStatus}, got ${res.statusCode}`));
          return;
        }
        if (!contentType.includes(check.expectContentType)) {
          reject(new Error(`${check.name}: expected ${check.expectContentType}, got ${contentType || 'no content-type'}`));
          return;
        }
        try {
          const parsed = JSON.parse(body);
          if (!parsed.mqtt || parsed.mqtt.connected !== true) {
            reject(new Error(`${check.name}: API responded but MQTT is not connected`));
            return;
          }
        } catch (error) {
          reject(new Error(`${check.name}: response was not valid JSON`));
          return;
        }
        resolve(`${check.name}: ok`);
      });
    });
    req.on('timeout', () => {
      req.destroy(new Error(`${check.name}: timed out`));
    });
    req.on('error', reject);
  });
}

(async () => {
  for (const check of checks) {
    console.log(await request(check));
  }
})().catch((error) => {
  console.error(`FAIL: ${error.message}`);
  process.exit(1);
});
NODE

public_status="$(curl -fsS -o /dev/null -w '%{http_code}' https://healthcheck.ukmesh.com/api/bootstrap)"
if [[ "$public_status" != "200" ]]; then
  echo "FAIL: public healthcheck API returned HTTP $public_status" >&2
  exit 1
fi

echo "Public healthcheck API: ok"
