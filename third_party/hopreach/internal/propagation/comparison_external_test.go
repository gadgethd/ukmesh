// UK Mesh modifications, 2026-08-02. Licensed with HopReach under
// AGPL-3.0 plus Commons Clause.
package propagation

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"hopreach/internal/demgrid"
)

type comparisonGrid struct{ elevation float64 }

func (g comparisonGrid) At(_, _ float64) float64 { return g.elevation }

func comparisonTerrainGrid() *demgrid.Grid {
	const tilesWide, tilesHigh = 6, 10
	const width, height = tilesWide * 256, tilesHigh * 256
	elevations := make([]float32, width*height)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			elevations[y*width+x] = 30 + float32((x*17+y*31+(x*y)%97)%900)/3
		}
	}
	grid, err := demgrid.NewFromElev(7, 60, 35, tilesWide, tilesHigh, elevations)
	if err != nil {
		panic(err)
	}
	return grid
}

func comparisonSites(count int) []Site {
	rng := rand.New(rand.NewSource(868 + int64(count)))
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

var comparisonSink []float32

func comparisonDigest(margins []float32) (string, int) {
	hash := sha256.New()
	var word [4]byte
	finite := 0
	for _, margin := range margins {
		binary.LittleEndian.PutUint32(word[:], math.Float32bits(margin))
		_, _ = hash.Write(word[:])
		if !math.IsNaN(float64(margin)) {
			finite++
		}
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), finite
}

func assertMarginsEqual(t *testing.T, got, want []float32) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		gotNaN := math.IsNaN(float64(got[i]))
		wantNaN := math.IsNaN(float64(want[i]))
		if gotNaN || wantNaN {
			if gotNaN != wantNaN {
				t.Fatalf("margin[%d] = %v, want %v", i, got[i], want[i])
			}
			continue
		}
		if math.Abs(float64(got[i]-want[i])) > 1e-5 {
			t.Fatalf("margin[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func BenchmarkSmallRasterComparison(b *testing.B) {
	p := Params{FrequencyMHz: 868, TxPowerDBm: 22, TxAntennaGainDB: 3, RxSensitivityDB: -124, FadeMarginDB: 20, RxHeightM: 2, MarginGreenDB: 15}
	bounds := Bounds{South: 49.8, North: 61.2, West: -8.3, East: 1.8}
	grid := comparisonGrid{elevation: 80}
	terrainGrid := comparisonTerrainGrid()
	for _, fixture := range []struct {
		name                 string
		nodes, width, height int
	}{
		{name: "Tiny128", nodes: 128, width: 48, height: 54},
		{name: "Small512", nodes: 512, width: 64, height: 72},
	} {
		sites := comparisonSites(fixture.nodes)
		b.Run(fixture.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				comparisonSink = ComputeMarginsCPU(grid, sites, bounds, fixture.width, fixture.height, 100, p, nil)
			}
		})
	}
	b.Run("Terrain128", func(b *testing.B) {
		sites := comparisonSites(128)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			comparisonSink = ComputeMarginsCPU(terrainGrid, sites, bounds, 48, 54, 100, p, nil)
		}
	})
}

func TestSmallRasterComparisonDigest(t *testing.T) {
	p := Params{FrequencyMHz: 868, TxPowerDBm: 22, TxAntennaGainDB: 3, RxSensitivityDB: -124, FadeMarginDB: 20, RxHeightM: 2, MarginGreenDB: 15}
	margins := ComputeMarginsCPU(
		comparisonGrid{elevation: 80},
		comparisonSites(256),
		Bounds{South: 49.8, North: 61.2, West: -8.3, East: 1.8},
		48, 54, 100, p, nil,
	)
	digest, finite := comparisonDigest(margins)
	if digest != "c36912a3db5f781c1bd0854c3e55fa80994caa5b452b1fd850fbda5e7ae573ef" || finite != 2506 {
		t.Fatalf("flat raster digest=%s finite=%d, want stable upstream result", digest, finite)
	}
	terrainMargins := ComputeMarginsCPU(
		comparisonTerrainGrid(),
		comparisonSites(128),
		Bounds{South: 49.8, North: 61.2, West: -8.3, East: 1.8},
		48, 54, 100, p, nil,
	)
	digest, _ = comparisonDigest(terrainMargins)
	if digest != "5cf9413b8ad2937866d42f8944dedfdb8bf06aefef9a0e6c03a0333832e0a9db" {
		t.Fatalf("terrain raster digest=%s, want stable upstream result", digest)
	}
}

func TestTerrainRasterMatchesUpstreamReference(t *testing.T) {
	bounds := Bounds{South: 49.8, North: 61.2, West: -8.3, East: 1.8}
	grid := comparisonTerrainGrid()
	sites := comparisonSites(96)
	fixtures := []struct {
		name    string
		rangeKm float64
		params  Params
	}{
		{name: "868MHz", rangeKm: 100, params: Params{FrequencyMHz: 868, TxPowerDBm: 22, TxAntennaGainDB: 3, RxSensitivityDB: -124, FadeMarginDB: 20, RxHeightM: 2}},
		{name: "433MHz", rangeKm: 70, params: Params{FrequencyMHz: 433, TxPowerDBm: 20, TxAntennaGainDB: 2.5, RxAntennaGainDB: 1, RxSensitivityDB: -120, FadeMarginDB: 12, RxHeightM: 1.5}},
		{name: "915MHz", rangeKm: 35, params: Params{FrequencyMHz: 915, TxPowerDBm: 18, TxAntennaGainDB: 4, RxSensitivityDB: -118, FadeMarginDB: 8, RxHeightM: 3}},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			want := ComputeMarginsCPUReference(grid, sites, bounds, 36, 40, fixture.rangeKm, fixture.params, nil)
			got := ComputeMarginsCPU(grid, sites, bounds, 36, 40, fixture.rangeKm, fixture.params, nil)
			assertMarginsEqual(t, got, want)
		})
	}
}
