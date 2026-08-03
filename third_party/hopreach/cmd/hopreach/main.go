// Command hopreach fetches repeater nodes from a CoreScope instance, keeps
// only the ones that fall geographically within the configured region and
// are scoped to the configured MeshCore region, then computes a
// terrain-aware estimated RF coverage map (free-space path loss +
// single-knife-edge diffraction over real elevation data) and writes:
//   - repeaters.geojson: the filtered repeater points
//   - coverage.png + bounds in meta.json: the estimated coverage heatmap
//   - meta.json: summary stats for the frontend
//   - progress.json: updated throughout the run so the frontend can show a
//     progress bar during the (potentially multi-minute) terrain analysis
//
// Configuration is a single YAML file — see internal/config — resolved
// from -config, then $HOPREACH_CONFIG, then ./config.yaml. -prepare renders
// the frontend's config.js, nginx's site config, and the cron file from
// that same config instead of running the fetch/compute pipeline; the
// Docker entrypoint calls it once at container startup.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	yconfig "hopreach/internal/config"
	"hopreach/internal/sysinfo"
)

func main() {
	// See sysinfo.ApplyGoMemoryLimit: without this, Go's GC has no
	// awareness of this container's real memory ceiling at all, and a real
	// Precision-tier pass's large buffers OOM-killed the container even
	// after being shrunk, right up against the exact cgroup limit.
	sysinfo.ApplyGoMemoryLimit()

	configFlag := flag.String("config", "", "path to config.yaml (default: $HOPREACH_CONFIG, then ./config.yaml)")
	force := flag.Bool("force", false, "run even if within coverage.min_recompute_interval_hours (each tier still skips itself if it already completed today — see -force-all-tiers)")
	forceAllTiers := flag.Bool("force-all-tiers", false, "also recompute every tier regardless of same-day freshness (implies -force); use after a config change that invalidates existing output, not for a routine restart/deploy")
	prepare := flag.Bool("prepare", false, "render config.js, nginx's site config, and the cron file from config.yaml, then exit")
	nodeFlag := flag.String("node", "", "compute standard-tier coverage for one 64-hex repeater public key, then exit")
	flag.Parse()
	if *prepare && *nodeFlag != "" {
		log.Fatal("hopreach: -prepare and -node cannot be used together")
	}
	var nodePublicKey string
	if *nodeFlag != "" {
		var err error
		nodePublicKey, err = normalizeNodePublicKey(*nodeFlag)
		if err != nil {
			log.Fatalf("hopreach: -node: %v", err)
		}
	}

	yc, path, err := yconfig.Load(*configFlag)
	if err != nil {
		log.Fatalf("hopreach: %v", err)
	}

	if *prepare {
		if err := runPrepare(yc, path); err != nil {
			log.Fatalf("hopreach: -prepare: %v", err)
		}
		return
	}

	// Excludes concurrent runs from whichever combination of the container's
	// initial background run, the daily cron job, and an on-demand
	// /admin/recompute trigger happens to overlap — see lock.go.
	lock, err := acquireLock()
	if err != nil {
		if nodePublicKey != "" {
			_ = json.NewEncoder(os.Stdout).Encode(nodeRunResult{PublicKey: nodePublicKey, State: "busy", Message: err.Error()})
			return
		}
		log.Printf("hopreach: %v, skipping this run", err)
		return
	}
	defer lock.Close()

	cfg := toAppConfig(yc)
	cfg.forceRecompute = *force || *forceAllTiers
	cfg.forceAllTiers = *forceAllTiers
	if nodePublicKey != "" {
		result, runErr := runNode(cfg, nodePublicKey)
		if encodeErr := json.NewEncoder(os.Stdout).Encode(result); encodeErr != nil {
			log.Fatalf("hopreach: writing node result: %v", encodeErr)
		}
		if runErr != nil {
			fmt.Fprintf(os.Stderr, "hopreach: node coverage: %v\n", runErr)
			os.Exit(1)
		}
		return
	}
	if err := run(cfg); err != nil {
		log.Fatalf("hopreach: %v", err)
	}
}
