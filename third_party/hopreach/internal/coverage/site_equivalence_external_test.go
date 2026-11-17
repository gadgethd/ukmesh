// UK Mesh modifications. Licensed with HopReach under AGPL-3.0 plus Commons Clause.
package coverage_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"image"
	"image/png"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"hopreach/internal/compute"
	"hopreach/internal/coverage"
	"hopreach/internal/demgrid"
	"hopreach/internal/propagation"
)

func equivalenceGrid(tb testing.TB, terrain bool) *demgrid.Grid {
	tb.Helper()
	const wide, high = 6, 10
	elevations := make([]float32, wide*high*256*256)
	for y := 0; y < high*256; y++ {
		for x := 0; x < wide*256; x++ {
			elevation := float32(80)
			if terrain {
				elevation = 30 + float32((x*17+y*31+(x*y)%97)%900)/3
			}
			elevations[y*wide*256+x] = elevation
		}
	}
	grid, err := demgrid.NewFromElev(7, 60, 35, wide, high, elevations)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { grid.Close() })
	return grid
}

func equivalenceSites(grid *demgrid.Grid) []propagation.Site {
	coordinates := [][2]float64{{51.5, -0.1}, {54.9, -2}, {57.2, -4.5}, {52, -2}}
	var sites []propagation.Site
	for repeat := 0; repeat < 10; repeat++ {
		for _, point := range coordinates {
			ground := grid.At(point[0], point[1])
			sites = append(sites, propagation.Site{Lat: point[0], Lon: point[1], GroundM: ground, TxHeightM: ground + 1})
		}
	}
	// A second height at the same position must remain an independent path.
	taller := sites[0]
	taller.TxHeightM += 50
	sites = append(sites, taller)
	return sites
}

func siteBits(sites []propagation.Site) [][4]uint64 {
	bits := make([][4]uint64, len(sites))
	for i, site := range sites {
		bits[i] = [4]uint64{math.Float64bits(site.Lat), math.Float64bits(site.Lon), math.Float64bits(site.GroundM), math.Float64bits(site.TxHeightM)}
	}
	return bits
}

func marginBytes(values []float32) []byte {
	data := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(value))
	}
	return data
}

func TestEquivalentSitesRasterAndPNGParity(t *testing.T) {
	// Captured from the optimized CPU before equivalence grouping was added.
	// The existing executable upstream oracle still owns its 1e-5 dB gate.
	golden := map[string][2]string{
		"standard-flat":     {"948f6c0035355d09caf2105afee805d295557fafdd8acba9987884a24b35b485", "3d1c5dc9ed0a36cac4f400e4d1ae02c78bf5cabcc5f8d6c72253814dbf54702e"},
		"precision-terrain": {"cf9b369a1ec31b174ec432c36f8208fb359f6507aac04763a53b23c6ee9db5fd", "0c80d845d8c17afbe8596575dcb57d9a027adf040363e855b262ebc841802915"},
		"433MHz-terrain":    {"cf7f34b0f630ed3631b35bb9863569e72ebf093e7cb73e55e0126d164f8eacb2", "34cee0d9cf89fe8b3eab3b7711cb06327f45c94abde247c93beba9782649e5ae"},
	}
	for _, fixture := range []struct {
		name        string
		terrain     bool
		supersample int
		frequency   float64
	}{
		{"standard-flat", false, 1, 868},
		{"precision-terrain", true, 2, 868},
		{"433MHz-terrain", true, 1, 433},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			grid := equivalenceGrid(t, fixture.terrain)
			sites := equivalenceSites(grid)
			original, _ := json.Marshal(siteBits(sites))
			bounds := propagation.Bounds{South: 49.8, North: 61.2, West: -8.3, East: 1.8}
			params := propagation.Params{FrequencyMHz: fixture.frequency, TxPowerDBm: 22, TxAntennaGainDB: 3,
				RxSensitivityDB: -124, FadeMarginDB: 20, AntennaHeightM: 1, RxHeightM: 2, MaxRangeKm: 100, MarginGreenDB: 15}
			var rendered *image.NRGBA
			if fixture.supersample == 1 {
				rendered = coverage.Raster(compute.New(), grid, sites, bounds, 64, params, 190, nil)
			} else {
				rendered = coverage.RasterSupersampled(compute.New(), grid, sites, bounds, 64, fixture.supersample, params, 190, nil)
			}
			width, height := rendered.Bounds().Dx()*fixture.supersample, rendered.Bounds().Dy()*fixture.supersample
			margins := propagation.ComputeMarginsCPU(grid, sites, bounds, width, height, propagation.LinkBudgetMaxRangeKm(params), params, nil)
			finite := 0
			for _, value := range margins {
				if !math.IsNaN(float64(value)) && !math.IsInf(float64(value), 0) {
					finite++
				}
			}
			if finite == 0 {
				t.Fatal("positive control produced only uncovered pixels")
			}
			var encoded bytes.Buffer
			if err := png.Encode(&encoded, rendered); err != nil {
				t.Fatal(err)
			}
			raw := marginBytes(margins)
			actual := [2]string{fmt.Sprintf("%x", sha256.Sum256(raw)), fmt.Sprintf("%x", sha256.Sum256(encoded.Bytes()))}
			if directory := os.Getenv("HOPREACH_GOLDEN_OUTPUT"); directory != "" {
				if err := os.MkdirAll(directory, 0700); err != nil {
					t.Fatal(err)
				}
				for extension, data := range map[string][]byte{"f32": raw, "png": encoded.Bytes()} {
					if err := os.WriteFile(filepath.Join(directory, fixture.name+"."+extension), data, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if actual != golden[fixture.name] {
				t.Errorf("full raster/PNG digests = %q, want %q; pixels=%d finite=%d", actual, golden[fixture.name], len(margins), finite)
			}
			after, _ := json.Marshal(siteBits(sites))
			if !bytes.Equal(original, after) {
				t.Fatal("CPU calculation mutated identity/site input")
			}
		})
	}
}

type equivalenceCountingGrid struct{ calls atomic.Int64 }

func (grid *equivalenceCountingGrid) At(_, _ float64) float64 {
	grid.calls.Add(1)
	return 80
}

func TestEquivalentSitesAvoidRepeatedTerrainWork(t *testing.T) {
	site := propagation.Site{Lat: 51.5, Lon: -0.1, GroundM: 80, TxHeightM: 81}
	unique := []propagation.Site{site}
	duplicates := []propagation.Site{site, site, site, site}
	bounds := propagation.Bounds{South: 51.49, North: 51.51, West: -0.11, East: -0.09}
	params := propagation.Params{FrequencyMHz: 868, TxPowerDBm: 22, TxAntennaGainDB: 3, RxSensitivityDB: -124, FadeMarginDB: 20, RxHeightM: 2}
	first, repeated := &equivalenceCountingGrid{}, &equivalenceCountingGrid{}
	want := propagation.ComputeMarginsCPU(first, unique, bounds, 8, 8, 100, params, nil)
	var progress [][2]int
	got := propagation.ComputeMarginsCPU(repeated, duplicates, bounds, 8, 8, 100, params,
		func(done, total int) { progress = append(progress, [2]int{done, total}) })
	if !bytes.Equal(marginBytes(want), marginBytes(got)) {
		t.Fatal("identical sites changed best-server margins")
	}
	if repeated.calls.Load() != first.calls.Load() {
		t.Fatalf("duplicate paths repeated terrain work: %d reads, unique=%d", repeated.calls.Load(), first.calls.Load())
	}
	if len(progress) != 8 {
		t.Fatalf("row completion count changed: %v", progress)
	}
	for i, step := range progress {
		if step != [2]int{i + 1, 8} {
			t.Fatalf("row progress changed: %v", progress)
		}
	}
	// Keeping a second transmitter height matters even at identical coordinates.
	taller := site
	taller.TxHeightM += 50
	negative := &equivalenceCountingGrid{}
	propagation.ComputeMarginsCPU(negative, []propagation.Site{site, taller}, bounds, 8, 8, 100, params, nil)
	if negative.calls.Load() <= first.calls.Load() {
		t.Fatal("different transmitter heights were conflated")
	}
}

func BenchmarkEquivalentSites(b *testing.B) {
	var sites []propagation.Site
	if fixture := os.Getenv("HOPREACH_DUPLICATE_FIXTURE"); fixture != "" {
		data, err := os.ReadFile(fixture)
		if err != nil {
			b.Fatal(err)
		}
		if err := json.Unmarshal(data, &sites); err != nil {
			b.Fatal(err)
		}
		b.Logf("complete owned fixture: %d resolved sites", len(sites))
	} else {
		for repeat := 0; repeat < 10; repeat++ {
			for i := 0; i < 128; i++ {
				sites = append(sites, propagation.Site{Lat: 50 + float64(i%16)/2, Lon: -6 + float64(i/16)/2, GroundM: 80, TxHeightM: 81})
			}
		}
	}
	if len(sites) == 0 {
		b.Fatal("the duplicate fixture must be populated")
	}
	unique := make(map[[4]uint64]bool)
	for _, bits := range siteBits(sites) {
		unique[bits] = true
	}
	grid := equivalenceGrid(b, false)
	params := propagation.Params{FrequencyMHz: 868, TxPowerDBm: 22, TxAntennaGainDB: 3, RxSensitivityDB: -124,
		FadeMarginDB: 20, AntennaHeightM: 1, RxHeightM: 2, MaxRangeKm: 100, MarginGreenDB: 15}
	bounds := propagation.Bounds{South: 48.73066365796828, North: 60.02124434203172, West: -8.746931150309857, East: 2.8996077488125778}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		propagation.ComputeMarginsCPU(grid, sites, bounds, 64, 72, propagation.LinkBudgetMaxRangeKm(params), params, nil)
	}
	b.StopTimer()
	b.ReportMetric(float64(len(sites)), "sites")
	b.ReportMetric(float64(len(unique)), "unique_sites")
}
