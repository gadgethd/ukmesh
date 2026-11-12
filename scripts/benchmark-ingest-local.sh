#!/usr/bin/env bash
# Recreates ONLY the named local sample stack twice. Never targets a remote DB.
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${UKMESH_LOCAL_ENV_CLEAN:-} != 1 ]]; then
  exec env -i PATH="$PATH" HOME="$HOME" LANG=C.UTF-8 UKMESH_LOCAL_ENV_CLEAN=1 \
    bash "$repo/scripts/benchmark-ingest-local.sh" "$@"
fi
cd "$repo"
label=${1:-}
[[ $label =~ ^[a-z0-9][a-z0-9-]*$ ]] || { echo 'Supply a new lowercase measurement label' >&2; exit 2; }
shift
data_dir= flush_ms=10
while (($#)); do
  case "$1" in
    --data-dir) data_dir=$(realpath "$2"); shift 2 ;;
    --flush-ms) flush_ms=$2; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done
[[ $flush_ms =~ ^[0-9]+$ && $flush_ms -le 1000 ]] || { echo '--flush-ms must be 0..1000' >&2; exit 2; }
output="$repo/plans/measurements/$label"
[[ ! -e $output ]] || { echo "Evidence already exists: $output" >&2; exit 1; }
mkdir -p plans/measurements
exec 9>plans/measurements/benchmark.lock
flock -n 9 || { echo 'Another local benchmark is running' >&2; exit 1; }
mkdir -p "$output"
if [[ -z $data_dir ]]; then
  data_dir="$output/dataset"
  npm --prefix backend run sample:generate -- --data-dir "$data_dir" --count 2000 > "$output/generate.log" 2>&1
fi
# Refuse archived/private inputs: this fixed benchmark is exactly 2k invented ACKs.
node --input-type=module - "$data_dir" <<'JS'
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
const root=process.argv[2], fixture=JSON.parse(readFileSync(path.join(root,'fixture.json')));
if(fixture.synthetic!==true||fixture.count!==2000||fixture.seed!=='sequential-ack-v1') throw Error('Expected the invented 2k sequential ACK fixture');
for(const name of ['nodes','packets','raw_packets','node_links','node_status_samples']) {
  const file=name+'.csv.gz';
  if(createHash('sha256').update(readFileSync(path.join(root,file))).digest('hex')!==fixture.checksums[file]) throw Error('Fixture checksum mismatch: '+file);
}
JS
cp "$data_dir/fixture.json" "$output/fixture.json"
git rev-parse HEAD > "$output/commit.txt"
git diff HEAD | sha256sum > "$output/diff.sha256"
git status --porcelain > "$output/git-status.txt"
printf '%s\n' "$flush_ms" > "$output/flush-ms.txt"
export DATABASE_URL=postgresql://meshcore:meshcore@127.0.0.1:5433/meshcore
for mode in saturation controlled; do
  mkdir "$output/$mode"
  scripts/dev-stack.sh reset --profile ingest --data-dir "$data_dir" --replay-limit 0 \
    --ingest-flush-ms "$flush_ms" > "$output/$mode/setup.log" 2>&1
  docker inspect ukmesh-sample-db ukmesh-sample-redis ukmesh-sample-mqtt \
    --format '{{.Name}} {{.Image}} {{json .HostConfig.Memory}} {{json .HostConfig.NanoCpus}}' > "$output/$mode/images.txt"
  cp plans/dev-stack/prepare.log "$output/$mode/prepare.log"
  cp scripts/local-sample/mosquitto-test.conf "$output/$mode/mosquitto.conf"
  docker exec ukmesh-sample-db cat /sys/fs/cgroup/cpu.stat > "$output/$mode/db-cpu-before.txt"
  speed=0
  if [[ $mode == controlled ]]; then speed=100; fi
  npm --prefix backend run bench:ingest -- --data-dir "$data_dir" --limit 2000 --speed "$speed" \
    --concurrency 16 --output "$output/$mode" > "$output/$mode/ingest.log" 2>&1
  docker exec ukmesh-sample-db cat /sys/fs/cgroup/cpu.stat > "$output/$mode/db-cpu-after.txt"
  docker logs ukmesh-sample-mqtt > "$output/$mode/mqtt.log" 2>&1
done
npm --prefix backend run bench:decode > "$output/decode.log" 2>&1
echo "Persisted ingest, query/WAL deltas, process samples and decode evidence: $output"
