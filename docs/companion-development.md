# UKMesh, CoreScope and Beacon in DEV

The companion repositories are Git submodules. Each has its own branches,
changes, commits and remotes; UKMesh records the commit to check out.
HopReach keeps its existing vendored layout.

| Directory | Fork | Upstream branch | Preserved UKMesh branch |
| --- | --- | --- | --- |
| `third_party/corescope` | `hermes-gadget/CoreScope` | `Kpa-clawbot/CoreScope:master` | `burn/corescope-hunt-20261023` |
| `third_party/beacon-server` | `gadgethd/beacon-server` | `MeshCore-Beacon/beacon-server:dev` | `security/remediation-20260912` |
| `third_party/beacon-web` | `gadgethd/beacon-web` | `MeshCore-Beacon/beacon-web:dev` | `security/remediation-20260912` |

The local integration branch in each submodule is `ukmesh-dev`. Integration
commits are local until explicitly pushed to each fork; publish those commits
before sharing the UKMesh submodule pins. These branches
merge upstream without rewriting the fork's existing commits. CoreScope's
preserved branch is the published recovery reconstruction, as documented in
that repository; it does not establish the ancestry of the separate deployment.
Beacon 2 replaces the 1.x migration chain and requires its own fresh database.
The DEV launcher creates that database independently of UKMesh.

## Start and edit

Docker Engine with Compose, Git, Node and npm are required. Go builds run in a
pinned Docker toolchain with cgo enabled; Go need not be installed on the host.
Run Docker commands with the account's Docker group access.

```sh
bash scripts/dev-all.sh init
bash scripts/dev-all.sh start
bash scripts/dev-all.sh status
```

`start` keeps an already healthy UKMesh fixture running. Otherwise it resumes
the prepared fixture, or creates one using the existing DEV stack command.
There is no reset or cleanup action in this launcher.

| App | Local URL |
| --- | --- |
| UKMesh | <http://127.0.0.1:5173> |
| UKMesh API | <http://127.0.0.1:3100> |
| CoreScope | <http://127.0.0.1:3102> |
| Beacon | <http://127.0.0.1:5175> |
| Beacon API | <http://127.0.0.1:3103/api/v1/info> |

All published companion ports bind to loopback. Beacon PostgreSQL is on
`127.0.0.1:5435`; Redis remains inside the companion network. CoreScope owns a
separate SQLite file under ignored `plans/companion-dev/runtime/corescope`.
Beacon owns the `ukmesh-companions_beacon-db` volume. The launcher's local
credentials are fixture values, defined in `scripts/companion-dev/compose.yml`.

Both ingestors subscribe to the existing **local sample broker**. They receive
new fixture traffic together with UKMesh. Starting them does not copy UKMesh's
historical database or replay packets. Each app keeps its own data model.
CoreScope's restricted PostgreSQL/public-stream adapters remain in the source,
but this fixture configuration uses MQTT and does not grant it UKMesh database
or Redis credentials. CoreScope uses its built-in OSM basemap option for this
fixture, so its default map does not need a CARTO API key. The production network
and deployment Compose files are
not used.

Edit any repository directly in its `third_party` checkout. Beacon Web uses
Vite with hot reload. CoreScope serves the checkout's static files; refresh the
browser after editing them. Rebuild and restart a Go service after changing it:

```sh
bash scripts/dev-all.sh restart corescope-ingestor
bash scripts/dev-all.sh restart corescope
bash scripts/dev-all.sh restart beacon
bash scripts/dev-all.sh logs beacon
```

`build` builds the Go binaries without starting services. The launcher discards
inherited application settings and passes Compose an empty env file. The apps
use explicit fixture configuration, and Beacon Web selects an empty Vite env
directory. A deployment `.env` is not needed.

```sh
bash scripts/dev-all.sh stop
bash scripts/dev-all.sh start
```

`stop` stops companions and the prepared UKMesh fixture and retains their data.
Use `companions` instead of `start` to start only companions against an already
running sample broker.

## Commit and update independently

Commit a change inside the repository that owns it. Run that repository's
checks, then publish its development branch before publishing a new UKMesh
submodule pin. This keeps the pin fetchable by another checkout.

```sh
git -C third_party/beacon-server status
git -C third_party/beacon-server add path/to/changed-file.go
git -C third_party/beacon-server commit -m "Describe the Beacon change"
git -C third_party/beacon-server push origin ukmesh-dev

git add third_party/beacon-server
git commit -m "Update Beacon development revision"
```

`init` registers missing upstream remotes and creates a development branch for
newly initialized, detached submodules. It preserves existing checkouts and
pending edits. Updating UKMesh's recorded pins may leave a submodule detached;
check its status before editing and explicitly return to the intended branch.

Fetch and merge upstream inside each repository when an update is wanted:

```sh
git -C third_party/corescope fetch upstream
git -C third_party/corescope merge upstream/master

git -C third_party/beacon-server fetch upstream
git -C third_party/beacon-server merge upstream/dev

git -C third_party/beacon-web fetch upstream
git -C third_party/beacon-web merge upstream/dev
```

Resolve overlaps, run checks and rebuild before updating pins. The launcher
never pulls, merges, force-pushes or resets source branches automatically.
The licenses and attribution remain in each checkout.
