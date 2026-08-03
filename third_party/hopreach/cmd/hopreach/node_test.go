package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"hopreach/internal/corescope"
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
