# UKMesh DEV LAN access

The optional dashboard and gateway use the existing DEV stack. Start the main stack first. The addon binds loopback by default. To allow app access from the LAN, pass a private IPv4 address assigned to the DEV host.

```bash
sg docker -c 'bash scripts/dev-all.sh lan start'
sg docker -c 'bash scripts/dev-all.sh lan start 192.168.2.22'
sg docker -c 'bash scripts/dev-all.sh lan status'
sg docker -c 'bash scripts/dev-all.sh lan stop'
```

Open port 8080 on the selected address. The app, CoreScope, public website, Beacon web, development website and Health Check retain ports 8081–8084, 8087 and 8089. Grafana, Prometheus and direct Beacon API access retain loopback ports 8085, 8086 and 8088. The app gateway restricts operator routes on the LAN. Normal app sessions and same-origin WebSockets continue through the app ports.

Starting or stopping the addon updates the local backend and Health Check origin allowlists. The route manifest also supplies the dashboard's readiness probes. All visitors share a refresh interval of five seconds.

Stop the addon before selecting a qualification fixture. Fixture selection checks this under the target lease. The addon's capped processes remain visible in whole-VM resource samples whenever running. After a VM reboot, restore the original DEV stores through the canonical launcher before enabling LAN access again.
