#!/bin/sh
# Reproducible HopReach UK raster release gate. Requires Go 1.23+.
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
source_dir="$repo_dir/third_party/hopreach"
profile_dir=${HOPREACH_PROFILE_DIR:-}
if [ -z "$profile_dir" ]; then
  profile_dir=$(mktemp -d "${TMPDIR:-/tmp}/hopreach-profiles.XXXXXX")
fi
mkdir -p "$profile_dir"

cd "$source_dir"
export GOMAXPROCS=${HOPREACH_CPU_WORKERS:-4}

all_output="$profile_dir/all-fixtures.txt"
gate_output="$profile_dir/uk-release-gate.txt"

go test ./internal/propagation -run '^$' \
  -bench 'BenchmarkRasterUKFixtures' -benchtime=1x -count=1 -benchmem \
  > "$all_output"

go test ./internal/propagation -run '^$' \
  -bench 'BenchmarkRasterUKFixtures/(Optimized|UpstreamReference)/(Standard|PrecisionTile)/UK4600$' \
  -benchtime=1x -count=3 -benchmem > "$gate_output"

median_ns() {
  pattern=$1
  awk -v pattern="$pattern" '
    $1 ~ pattern { values[++count] = $3 }
    END {
      if (count == 0) exit 1
      for (i = 2; i <= count; i++) {
        value = values[i]
        j = i - 1
        while (j >= 1 && values[j] > value) {
          values[j + 1] = values[j]
          j--
        }
        values[j + 1] = value
      }
      middle = int((count + 1) / 2)
      if (count % 2 == 1) printf "%.0f", values[middle]
      else printf "%.0f", (values[middle] + values[middle + 1]) / 2
    }
  ' "$gate_output"
}

check_time_gate() {
  tier=$1
  optimized=$(median_ns "Optimized/$tier/UK4600-")
  reference=$(median_ns "UpstreamReference/$tier/UK4600-")
  awk -v optimized="$optimized" -v reference="$reference" -v tier="$tier" 'BEGIN {
    ratio = optimized / reference
    printf "%s: optimized %.3fs, upstream %.3fs, ratio %.3f\n", tier, optimized/1e9, reference/1e9, ratio
    if (ratio > 0.70) {
      printf "FAIL: %s needs at least a 30%% raster-time reduction\n", tier > "/dev/stderr"
      exit 1
    }
  }'
}

check_time_gate Standard
check_time_gate PrecisionTile

# The memory gate uses the exact float32 DEM grid bytes that demgrid.Load maps
# for the versioned UK boundary. It includes full-range tile padding.
go test ./internal/coverage -run '^TestUKProgressiveTerrainWorkingSetReleaseGate$' -v \
  > "$profile_dir/memory-gate.txt"
go test ./internal/coverage -run '^$' -bench '^BenchmarkUKTerrainWorkingSet$' -benchtime=1x \
  > "$profile_dir/memory-working-set.txt"

# Separate profiles make upstream and optimized costs independently inspectable.
go test ./internal/propagation -run '^$' \
  -bench 'BenchmarkRasterUKFixtures/Optimized/(Standard|PrecisionTile)/UK4600$' \
  -benchtime=1x -count=1 -cpuprofile "$profile_dir/optimized.cpu.pprof" \
  -memprofile "$profile_dir/optimized.mem.pprof" \
  > "$profile_dir/optimized-profile.txt"
go test ./internal/propagation -run '^$' \
  -bench 'BenchmarkRasterUKFixtures/UpstreamReference/(Standard|PrecisionTile)/UK4600$' \
  -benchtime=1x -count=1 -cpuprofile "$profile_dir/upstream.cpu.pprof" \
  -memprofile "$profile_dir/upstream.mem.pprof" \
  > "$profile_dir/upstream-profile.txt"

# Scheduler trace records progressive tile/checkpoint completion behaviour.
go test ./internal/coverage \
  -run 'Test(PlanPublicationTilesCoversRasterWithoutGaps|ProgressiveSignatureChangesWithRFInputs)$' \
  -trace "$profile_dir/tile-completion.trace" \
  > "$profile_dir/tile-completion.txt"

printf 'PASS: HopReach UK speed and memory gates\nProfiles: %s\n' "$profile_dir"
