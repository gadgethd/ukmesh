# Switch the DEV interface

Check the current and previous releases from the UKMesh checkout.

```bash
bash scripts/dev-all.sh ui status
```

Set `release_id` to a sealed candidate in `plans/ui-overhaul/releases` before
applying it.

```bash
bash scripts/dev-all.sh ui apply "$release_id"
```

Switch back to the recorded previous release when needed.

```bash
bash scripts/dev-all.sh ui rollback
```

The launcher checks that the original DEV data stores are active. It verifies the
asset checksums and local images before switching the website, map, Health Check,
CoreScope web, Beacon Web and the DEV hub. A failed startup or HTTP check
restores the prior UI on the affected services. Normal DEV starts use the
selected release. The companion cutover routes have separate controls.

The primary LAN website serves CoreScope at `/live-map`, Health Check at
`/health-check` and Beacon channels at `/feed`. The UKMesh header stays visible.
The Packets tab opens the existing packet feed at `/feed/packets`. The tools
keep their own origins and cookies. The LAN gateway allows their frames only
on the bound UKMesh website. Run `bash scripts/dev-all.sh lan refresh` to
install an updated gateway policy without restarting backend or MQTT services.
The standalone tools and the legacy map remain available through the DEV hub.
On the existing DEV VM, `ukmesh-ui-20261126-2` identifies the recorded
pre-integration website release if its sealed files and local images are
still present. Use `ui status` to identify the previous release on this host.

The integrated chat uses the existing LAN-only Beacon service. The public
Beacon projection still requires a matching privacy generation and registry
revision. The LAN frame does not publish it.

Release files live under `plans/ui-overhaul/releases`. The classic release
contains the captured HTML, scripts, styles and static files. Vite releases
share the current and classic hashed assets so tabs opened before a switch can
load a feature afterward. Keep the selected and rollback releases on disk along
with their local images.

## Prepare a candidate

1. Start the complete managed DEV stack and its LAN gateway on the original
   DEV stores. On a fresh checkout, this gives you the baseline for a UI
   rollback. Capture the currently served UI files and image IDs before
   changing any live-mounted sources. The local capture goes under
   `plans/ui-overhaul/baseline/<container>/<asset-directory>`. The
   [assembler](../scripts/full-dev/ui/assemble.py) defines the asset directory
   names and the [release controller](../scripts/full-dev/ui/control.py)
   defines the container names and mounts.
2. Build the app, public website, test website, and both Beacon Web modes.
   Place the checked builds in the candidate's `app`, `website`,
   `website-dev`, and `beacon-web` directories.
3. Run `python3 scripts/full-dev/ui/assemble.py <release-id>` to package Health
   Check, CoreScope, the DEV hub and the shared theme. Use
   `--classic-name <new-classic-id>` to prepare a new classic bundle when the
   candidate needs additional retained assets.
4. Seal the baseline with `bash scripts/dev-all.sh ui seal classic` on the
   first rollout, while its original images are present. Seal the candidate
   with `bash scripts/dev-all.sh ui seal "$release_id"` while the intended
   candidate images are present. The controller records the image IDs from
   the named service containers at seal time. Apply verifies both the new
   release and the active one. On the first apply, it uses the sealed
   `classic` release as the previous release. Keep both sets of files, both
   image sets and the browser verification results.
