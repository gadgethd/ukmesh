# UKMesh DEV — LAN access layer

A small, read-only presentation layer over the canonical DEV stack. It adds no second
data plane: it joins the existing `ukmesh-full_default` / `ukmesh-companions_default`
networks and publishes fixed ports on all interfaces.

| Port | Service | Upstream |
|---:|---|---|
| 8080 | Status dashboard | local `site` container |
| 8081 | MeshCore app | `app-ukmesh:8080` |
| 8082 | CoreScope | `corescope:3000` |
| 8083 | UKMesh website | `website-ukmesh:8080` |
| 8084 | Beacon web | `beacon-web:8080` |
| 8085 | Grafana | `grafana:3000` |
| 8086 | Prometheus | `prometheus:9090` |
| 8087 | Website (dev) | `website-dev:8080` |
| 8088 | Beacon API | `beacon:8080` |
| 8089 | Mesh health check | `mesh-health-check:3090` |

The dashboard probes each service server-side every 5 seconds and renders link cards
(HTTP < 500 counts as reachable, so auth-protected pages still show as up).

## Usage

```bash
sg docker -c "docker compose -f scripts/full-dev/lan/compose.yml up -d"
sg docker -c "docker compose -f scripts/full-dev/lan/compose.yml ps"
sg docker -c "docker compose -f scripts/full-dev/lan/compose.yml down"
```

Then open `http://<this-host-LAN-IP>:8080/`.

Memory: ~64-96 MiB caps per container, ~30-50 MiB typical total. Images are pinned to the
same node digest the repo already builds with, plus nginx 1.27
(`sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10`).

## Notes

- If the stack's DB/MQTT/Redis are down, the app will show down while Grafana/Prometheus
  may still answer; the dashboard reflects each service independently.
- The gateway re-resolves container names through Docker DNS (10 s), so stack restarts do
  not require restarting the gateway.
- This is not a data clone. A second full stack would roughly double the runtime working
  set (2.7 GiB peak) and break the soak memory discipline on this 3.8 GiB host.
