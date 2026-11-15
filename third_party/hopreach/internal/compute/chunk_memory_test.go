package compute

import (
	"math"
	"net/http"
	"runtime"
	"strconv"
	"testing"

	"hopreach/internal/propagation"
)

// Unreachable transmitters must not multiply the Precision planner's memory by
// the number of chunks. The same output requires no terrain requests.
func TestSparsePrecisionPlanningMemory(t *testing.T) {
	for _, count := range []int{1000, 31540} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			bounds := propagation.Bounds{South: 58.8, North: 60.1, West: -8.8, East: -6.7}
			sites := make([]propagation.Site, count)
			for i := range sites {
				sites[i] = propagation.Site{Lat: -40, Lon: 120 + float64(i)/10000}
			}
			engine := New()
			engine.SetChunkBudgetBytes(128_000_000)
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			done, total := 0, 0
			margins, err := engine.MarginsChunked(bounds, 13, t.TempDir(), "http://unreachable.invalid", &http.Client{}, sites, 2048, 2048, 78, propagation.Params{}, 2, func(d, n int) { done, total = d, n })
			runtime.ReadMemStats(&after)
			if err != nil {
				t.Fatal(err)
			}
			if len(margins) != 1024*1024 {
				t.Fatalf("wrong output size %d", len(margins))
			}
			for _, value := range margins {
				if !math.IsNaN(float64(value)) {
					t.Fatal("unreachable transmitter produced coverage")
				}
			}
			if done != total || total <= 0 {
				t.Fatalf("incomplete progress %d/%d", done, total)
			}
			allocated := after.TotalAlloc - before.TotalAlloc
			t.Logf("Precision planning allocated %d bytes for %d unreachable transmitters", allocated, len(sites))
			if allocated > 32<<20 {
				t.Fatalf("sparse Precision planning allocated %d bytes; budget %d", allocated, 32<<20)
			}
		})
	}
}
