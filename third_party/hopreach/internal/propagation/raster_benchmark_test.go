// UK Mesh raster benchmarks. Run with:
// go test ./internal/propagation -run '^$' -bench Raster -benchmem
package propagation

import (
	"fmt"
	"math/rand"
	"testing"
)

func benchmarkSites(count int) []Site {
	rng := rand.New(rand.NewSource(int64(count)))
	sites := make([]Site, count)
	for i := range sites {
		sites[i] = Site{
			Lat:       49.9 + rng.Float64()*11.5,
			Lon:       -8.2 + rng.Float64()*10.0,
			GroundM:   rng.Float64() * 450,
			TxHeightM: 5 + rng.Float64()*450,
		}
	}
	return sites
}

func BenchmarkRasterUKFixtures(b *testing.B) {
	p := Params{FrequencyMHz: 868, TxPowerDBm: 22, TxAntennaGainDB: 3, RxSensitivityDB: -124, FadeMarginDB: 20, RxHeightM: 2, MarginGreenDB: 15}
	bounds := Bounds{South: 49.8, North: 61.2, West: -8.3, East: 1.8}
	grid := ukFlatGrid{elevation: 80}
	for _, fixture := range []struct {
		name   string
		nodes  int
		width  int
		height int
	}{
		{name: "Standard/500", nodes: 500, width: 128, height: 144},
		{name: "Standard/UK4600", nodes: 4600, width: 128, height: 144},
		{name: "PrecisionTile/500", nodes: 500, width: 256, height: 256},
		{name: "PrecisionTile/UK4600", nodes: 4600, width: 256, height: 256},
	} {
		sites := benchmarkSites(fixture.nodes)
		b.Run(fmt.Sprintf("Optimized/%s", fixture.name), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ComputeMarginsCPU(grid, sites, bounds, fixture.width, fixture.height, 100, p, nil)
			}
		})
		b.Run(fmt.Sprintf("UpstreamReference/%s", fixture.name), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ComputeMarginsCPUReference(grid, sites, bounds, fixture.width, fixture.height, 100, p, nil)
			}
		})
	}
}
