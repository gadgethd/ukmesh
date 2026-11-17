package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hopreach/internal/compute"
	"hopreach/internal/corescope"
	"hopreach/internal/coverage"
)

func TestNormalizeNodePublicKey(t *testing.T) {
	upper := strings.Repeat("AB", 32)
	got, err := normalizeNodePublicKey("  " + upper + "  ")
	if err != nil || got != strings.ToLower(upper) {
		t.Fatalf("normalizeNodePublicKey() = %q, %v", got, err)
	}
	for _, invalid := range []string{"", strings.Repeat("a", 63), strings.Repeat("z", 64)} {
		if _, err := normalizeNodePublicKey(invalid); err == nil {
			t.Errorf("normalizeNodePublicKey(%q) accepted invalid input", invalid)
		}
	}
}

func TestNodeDatasetIDChangesWithNodeSnapshot(t *testing.T) {
	lat, lon := 55.9, -3.2
	heard := "2026-08-03T10:00:00Z"
	node := corescope.Node{Lat: &lat, Lon: &lon, LastHeard: &heard}
	cfg := appConfig{demZoom: 11, coverageImageWidth: 2000, coveragePublicationTileSize: 1024}
	first, err := nodeDatasetID(strings.Repeat("a", 64), node, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !nodeDatasetPattern.MatchString(first) || len(first) != 25 {
		t.Fatalf("dataset ID %q is not a bounded progressive tier", first)
	}
	lat = 56.0
	second, err := nodeDatasetID(strings.Repeat("a", 64), node, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("position change did not change dataset identity")
	}
}

func TestWriteNodeCoverageIndexPreservesGlobalRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.json")
	original := `{"generated_at":"2026-08-03T00:00:00Z","coverage":{"standard":{"tiles":[]}},"run":{"id":"global-run","tiers":{"standard":{"state":"available"}}}}`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	key := strings.Repeat("a", 64)
	index := map[string]*nodeCoverageMeta{key: {DatasetID: "n" + strings.Repeat("1", 24), State: "computing"}}
	if err := writeNodeCoverageIndex(dir, index); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	var run struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(document["run"], &run); err != nil {
		t.Fatal(err)
	}
	if run.ID != "global-run" {
		t.Fatalf("global run changed to %q", run.ID)
	}
	if _, ok := document["coverage"]; !ok {
		t.Fatal("global coverage was removed")
	}
}

func TestPruneNodeCoverageIsBoundedAndPreservesCurrent(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	index := make(map[string]*nodeCoverageMeta)
	for i := 0; i < nodeCoverageMaxEntries+10; i++ {
		key := fmt.Sprintf("%064x", i)
		index[key] = &nodeCoverageMeta{UpdatedAt: now.Add(-time.Duration(i) * time.Minute).Format(time.RFC3339)}
	}
	preserve := strings.Repeat("f", 64)
	index[preserve] = &nodeCoverageMeta{UpdatedAt: now.Add(-30 * 24 * time.Hour).Format(time.RFC3339)}
	pruneNodeCoverage(index, preserve, now)
	if len(index) > nodeCoverageMaxEntries {
		t.Fatalf("index has %d entries", len(index))
	}
	if index[preserve] == nil {
		t.Fatal("current request was pruned")
	}
}

func TestRunNodeParksAndKeepsLastPublishedCoverage(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	key := strings.Repeat("a", 64)
	lat, lon := 55.9533, -3.1883
	lastHeard := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	node := corescope.Node{PublicKey: key, Role: "repeater", Lat: &lat, Lon: &lon, LastHeard: &lastHeard}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{"nodes": []corescope.Node{node}, "total": 1})
		case "/gpu/status":
			_, _ = w.Write([]byte(`{"worker_connected":false}`))
		default:
			t.Errorf("unexpected request to %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	cfg := appConfig{
		apiURL: srv.URL, outputDir: dir, timeout: time.Second, demCacheDir: t.TempDir(),
		gpuMode: "remote", gpuBrokerAddr: strings.TrimPrefix(srv.URL, "http://"), demTileURLBase: srv.URL,
		activeHours: 2, degradedHours: 12, coverageImageWidth: 1024, coveragePublicationTileSize: 256,
	}
	datasetID, err := nodeDatasetID(key, node, cfg)
	if err != nil {
		t.Fatal(err)
	}
	requested := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339)
	lastPublished := "tiles/nodes/previous/0-0.png"
	index := map[string]*nodeCoverageMeta{
		key: {DatasetID: datasetID, State: "computing", RequestedAt: requested, CompletedTiles: 2, TotalTiles: 10,
			Standard: &coverageMeta{Tiles: []coverage.Tile{{Image: lastPublished}}}},
	}
	if err := writeNodeCoverageIndex(dir, index); err != nil {
		t.Fatal(err)
	}

	result, err := runNode(cfg, key)
	if !errors.Is(err, compute.ErrRemoteParked) || result.State != "parked" || result.DatasetID != datasetID {
		t.Fatalf("runNode returned %+v, %v", result, err)
	}
	index, err = readNodeCoverageIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	entry := index[key]
	if entry == nil || entry.State != "computing" || !entry.WaitingForGPU || entry.Failure != "" {
		t.Fatalf("expected a recoverable node entry, got %+v", entry)
	}
	if entry.RequestedAt != requested || entry.CompletedTiles != 2 || entry.TotalTiles != 10 {
		t.Fatalf("resume progress was changed: %+v", entry)
	}
	if entry.Standard == nil || len(entry.Standard.Tiles) != 1 || entry.Standard.Tiles[0].Image != lastPublished {
		t.Fatalf("last-good coverage was lost: %+v", entry.Standard)
	}
}

func TestNodeDatasetCleanupKeepsPendingCheckpointsAndPublishedTiles(t *testing.T) {
	dir := t.TempDir()
	checkpointDir := filepath.Join(dir, "checkpoints", nodeCoverageNamespace)
	if err := os.MkdirAll(checkpointDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"published", "parked", "computing", "abandoned"} {
		if err := os.MkdirAll(filepath.Join(dir, "tiles", nodeCoverageNamespace, id), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(checkpointDir, id+".json"), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cleanupUnreferencedNodeDatasets(dir, map[string]*nodeCoverageMeta{
		"first": {DatasetID: "parked", State: "computing", WaitingForGPU: true,
			Standard: &coverageMeta{Tiles: []coverage.Tile{{Image: "tiles/nodes/published/0-0.png"}}}},
		"second": {DatasetID: "computing", State: "computing"},
		"third":  {DatasetID: "abandoned", State: "failed"},
	})
	for _, id := range []string{"published", "parked", "computing"} {
		for _, path := range []string{filepath.Join(dir, "tiles", nodeCoverageNamespace, id), filepath.Join(checkpointDir, id+".json")} {
			if _, err := os.Stat(path); err != nil {
				t.Errorf("expected to keep %s: %v", path, err)
			}
		}
	}
	for _, path := range []string{filepath.Join(dir, "tiles", nodeCoverageNamespace, "abandoned"), filepath.Join(checkpointDir, "abandoned.json")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("expected to remove %s: %v", path, err)
		}
	}
}
