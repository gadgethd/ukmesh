// UK Mesh modifications, 2026-08-02. Licensed with HopReach under
// AGPL-3.0 plus Commons Clause.
package coverage

import (
	"strings"
	"testing"
	"time"

	"hopreach/internal/propagation"
)

func TestProgressiveNamespaceRejectsPathSegments(t *testing.T) {
	_, err := RasterProgressiveChunked(nil, propagation.Bounds{}, 11, "", "", nil, nil, 0, 1, propagation.Params{}, 0,
		ProgressiveOptions{Tier: "node", Namespace: "../nodes", RunID: "node-test"}, nil)
	if err == nil || !strings.Contains(err.Error(), "invalid progressive namespace") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPlanPublicationTilesCoversRasterWithoutGaps(t *testing.T) {
	bounds := propagation.Bounds{South: 49, North: 61, West: -9, East: 3}
	tiles := planPublicationTiles(bounds, 2000, 2401, 1024)
	if len(tiles) != 6 {
		t.Fatalf("tiles=%d, want 6", len(tiles))
	}
	covered := make([]bool, 2000*2401)
	for _, tile := range tiles {
		for y := tile.y0; y < tile.y1; y++ {
			for x := tile.x0; x < tile.x1; x++ {
				i := y*2000 + x
				if covered[i] {
					t.Fatalf("pixel %d,%d covered twice", x, y)
				}
				covered[i] = true
			}
		}
	}
	for i, ok := range covered {
		if !ok {
			t.Fatalf("pixel %d not covered", i)
		}
	}
}

func TestResumeProgressiveCheckpointKeepsOnlyMatchingIncompleteRun(t *testing.T) {
	started := time.Date(2026, 8, 2, 2, 17, 0, 0, time.UTC)
	tile := Tile{Image: "tiles/standard/0-0.png"}
	existing := &progressiveCheckpoint{
		RunID: "run-1", Tier: "standard", Signature: "sig-1",
		StartedAt: started.Format(time.RFC3339), Completed: map[string]Tile{"0-0.png": tile},
		TotalTiles: 4,
	}

	resumed, reset := resumeProgressiveCheckpoint(existing, "run-1", "standard", "sig-1", 6, started.Add(time.Hour))
	if reset || resumed != existing {
		t.Fatal("matching incomplete checkpoint was reset")
	}
	if got := resumed.Completed["0-0.png"].Image; got != tile.Image {
		t.Fatalf("completed tile was not retained: %q", got)
	}
	if resumed.TotalTiles != 6 || resumed.StartedAt != started.Format(time.RFC3339) {
		t.Fatalf("resume metadata changed unexpectedly: %+v", resumed)
	}

	for name, candidate := range map[string]*progressiveCheckpoint{
		"missing":   nil,
		"new run":   {RunID: "run-0", Tier: "standard", Signature: "sig-1"},
		"new tier":  {RunID: "run-1", Tier: "precision", Signature: "sig-1"},
		"new input": {RunID: "run-1", Tier: "standard", Signature: "sig-0"},
		"complete":  {RunID: "run-1", Tier: "standard", Signature: "sig-1", Complete: true},
	} {
		t.Run(name, func(t *testing.T) {
			fresh, wasReset := resumeProgressiveCheckpoint(candidate, "run-1", "standard", "sig-1", 6, started)
			if !wasReset || len(fresh.Completed) != 0 || fresh.Complete {
				t.Fatalf("invalid checkpoint was not reset: %+v", fresh)
			}
			if fresh.RunID != "run-1" || fresh.Tier != "standard" || fresh.Signature != "sig-1" || fresh.TotalTiles != 6 {
				t.Fatalf("fresh checkpoint metadata mismatch: %+v", fresh)
			}
		})
	}
}

func TestProgressiveSignatureChangesWithRFInputs(t *testing.T) {
	bounds := propagation.Bounds{South: 50, North: 51, West: -2, East: -1}
	p := propagation.Params{FrequencyMHz: 868, FadeMarginDB: 20}
	sites := []propagation.Site{{Lat: 50.5, Lon: -1.5, TxHeightM: 10}}
	first, err := progressiveSignature(bounds, 11, 2000, 2000, 1, sites, p)
	if err != nil {
		t.Fatal(err)
	}
	p.FadeMarginDB++
	second, err := progressiveSignature(bounds, 11, 2000, 2000, 1, sites, p)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("RF parameter change did not invalidate checkpoint signature")
	}
}
