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

type commandFlags struct {
	configPath    string
	force         bool
	forceAllTiers bool
	prepare       bool
	scheduled     bool
	node          string
}

func registerFlags(fs *flag.FlagSet) *commandFlags {
	opts := &commandFlags{}
	fs.StringVar(&opts.configPath, "config", "", "path to config.yaml (default: $HOPREACH_CONFIG, then ./config.yaml)")
	fs.BoolVar(&opts.force, "force", false, "run even if within coverage.min_recompute_interval_hours (each tier still skips itself if it already completed today — see -force-all-tiers)")
	fs.BoolVar(&opts.forceAllTiers, "force-all-tiers", false, "also recompute every tier regardless of same-day freshness (implies -force); use after a config change that invalidates existing output, not for a routine restart/deploy")
	fs.BoolVar(&opts.prepare, "prepare", false, "render config.js, nginx's site config, and the cron file from config.yaml, then exit")
	fs.BoolVar(&opts.scheduled, "scheduled", false, "wait up to one hour for the calculator lock and refresh unless a run completed since the latest daily cron slot")
	fs.StringVar(&opts.node, "node", "", "compute standard-tier coverage for one 64-hex repeater public key, then exit")
	return opts
}

func main() {
	// See sysinfo.ApplyGoMemoryLimit: without this, Go's GC has no
	// awareness of this container's real memory ceiling at all, and a real
	// Precision-tier pass's large buffers OOM-killed the container even
	// after being shrunk, right up against the exact cgroup limit.
	sysinfo.ApplyGoMemoryLimit()

	opts := registerFlags(flag.CommandLine)
	flag.Parse()
	if opts.prepare && opts.node != "" {
		log.Fatal("hopreach: -prepare and -node cannot be used together")
	}
	if opts.scheduled && (opts.prepare || opts.node != "") {
		log.Fatal("hopreach: -scheduled cannot be used with -prepare or -node")
	}
	var nodePublicKey string
	if opts.node != "" {
		var err error
		nodePublicKey, err = normalizeNodePublicKey(opts.node)
		if err != nil {
			log.Fatalf("hopreach: -node: %v", err)
		}
	}

	yc, path, err := yconfig.Load(opts.configPath)
	if err != nil {
		log.Fatalf("hopreach: %v", err)
	}

	if opts.prepare {
		if err := runPrepare(yc, path); err != nil {
			log.Fatalf("hopreach: -prepare: %v", err)
		}
		return
	}

	// Excludes concurrent runs from whichever combination of the container's
	// initial background run, the daily cron job, and an on-demand
	// /admin/recompute trigger happens to overlap — see lock.go.
	acquire := acquireLock
	if opts.scheduled {
		acquire = func() (*os.File, error) {
			return waitForLock(acquireLock, scheduledLockTimeout, scheduledLockPollInterval)
		}
	}
	lock, err := acquire()
	if err != nil {
		if opts.scheduled {
			log.Fatalf("hopreach: scheduled run failed: %v", err)
		}
		if nodePublicKey != "" {
			_ = json.NewEncoder(os.Stdout).Encode(nodeRunResult{PublicKey: nodePublicKey, State: "busy", Message: err.Error()})
			return
		}
		log.Printf("hopreach: %v, skipping this run", err)
		return
	}
	defer lock.Close()

	cfg := toAppConfig(yc)
	cfg.forceRecompute = opts.force || opts.forceAllTiers
	cfg.forceAllTiers = opts.forceAllTiers
	cfg.scheduled = opts.scheduled
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
