// UK Mesh modifications, 2026-08-02. Licensed with HopReach under
// AGPL-3.0 plus Commons Clause.
package propagation

import (
	"math"
	"math/rand"
	"slices"
	"testing"
)

type ukFlatGrid struct{ elevation float64 }

func (g ukFlatGrid) At(_, _ float64) float64 { return g.elevation }

func exactCandidateIndices(sites []Site, lat, lon, rangeKm float64) []int {
	out := make([]int, 0)
	for i, site := range sites {
		if HaversineKm(lat, lon, site.Lat, site.Lon) <= rangeKm {
			out = append(out, i)
		}
	}
	return out
}

func assertConservative(t *testing.T, sites []Site, points [][2]float64, rangeKm float64) {
	t.Helper()
	idx := NewSpatialIndex(sites, rangeKm)
	buf := make([]int, 0, len(sites))
	for _, point := range points {
		got := idx.Query(point[0], point[1], rangeKm, buf)
		buf = got
		gotSet := make(map[int]bool, len(got))
		for _, candidate := range got {
			gotSet[candidate] = true
		}
		for _, want := range exactCandidateIndices(sites, point[0], point[1], rangeKm) {
			if !gotSet[want] {
				t.Fatalf("index omitted reachable site %d (%v) from point %v at %.3f km", want, sites[want], point, rangeKm)
			}
		}
	}
}

func TestSpatialIndexConservativeExhaustiveGrid(t *testing.T) {
	var sites []Site
	var points [][2]float64
	for lat := 49.0; lat <= 62.5; lat += 0.25 {
		for lon := -11.0; lon <= 4.0; lon += 0.25 {
			sites = append(sites, Site{Lat: lat, Lon: lon})
			points = append(points, [2]float64{lat + 0.125, lon + 0.125})
		}
	}
	for _, rangeKm := range []float64{0, 0.01, 1, 25, 100} {
		assertConservative(t, sites, points, rangeKm)
	}
}

func TestSpatialIndexConservativeRandomisedGlobal(t *testing.T) {
	rng := rand.New(rand.NewSource(0x484f505245414348))
	for iteration := 0; iteration < 30; iteration++ {
		sites := make([]Site, 500)
		points := make([][2]float64, 200)
		for i := range sites {
			sites[i] = Site{Lat: rng.Float64()*178 - 89, Lon: rng.Float64()*360 - 180}
		}
		for i := range points {
			points[i] = [2]float64{rng.Float64()*178 - 89, rng.Float64()*360 - 180}
		}
		assertConservative(t, sites, points, 0.01+rng.Float64()*500)
	}
}

func TestSpatialIndexBoundariesPolesAndAntimeridian(t *testing.T) {
	longitudeDegreesAt55N := 100 / (EarthRadiusKm * math.Cos(55*math.Pi/180)) * 180 / math.Pi
	sites := []Site{
		{Lat: 54, Lon: -8}, {Lat: 54, Lon: 2},
		{Lat: 89.9, Lon: 179.9}, {Lat: 89.9, Lon: -179.9},
		{Lat: -89.9, Lon: 90}, {Lat: 0, Lon: 179.999}, {Lat: 0, Lon: -179.999},
		{Lat: 55, Lon: -3}, {Lat: 55, Lon: -3 + longitudeDegreesAt55N},
	}
	points := [][2]float64{
		{54, -8}, {54, 2}, {90, 0}, {-90, 0}, {89.9, -179.95},
		{0, 180}, {0, -180}, {55, -3},
	}
	assertConservative(t, sites, points, 100)
}

func TestIndexedCPUEqualsUpstreamReference(t *testing.T) {
	rng := rand.New(rand.NewSource(32))
	sites := make([]Site, 120)
	for i := range sites {
		sites[i] = Site{
			Lat:       54 + rng.Float64()*3,
			Lon:       -5 + rng.Float64()*4,
			GroundM:   rng.Float64() * 300,
			TxHeightM: 5 + rng.Float64()*300,
		}
	}
	bounds := Bounds{South: 54.5, North: 56.5, West: -4.5, East: -1.5}
	p := Params{FrequencyMHz: 868, TxPowerDBm: 22, TxAntennaGainDB: 3, RxSensitivityDB: -124, FadeMarginDB: 20, RxHeightM: 2, MarginGreenDB: 15}
	grid := ukFlatGrid{elevation: 75}
	want := ComputeMarginsCPUReference(grid, sites, bounds, 48, 40, 100, p, nil)
	got := ComputeMarginsCPU(grid, sites, bounds, 48, 40, 100, p, nil)
	if len(got) != len(want) {
		t.Fatalf("length = %d, want %d", len(got), len(want))
	}
	const tolerance = 1e-5
	for i := range want {
		if math.IsNaN(float64(want[i])) {
			if !math.IsNaN(float64(got[i])) {
				t.Fatalf("margin[%d] = %v, want NaN", i, got[i])
			}
			continue
		}
		if math.Abs(float64(got[i]-want[i])) > tolerance {
			t.Fatalf("margin[%d] = %v, want %v (tolerance %g)", i, got[i], want[i], tolerance)
		}
	}
}

func TestIndexedCPUReferenceParityEdgeFixtures(t *testing.T) {
	p := Params{FrequencyMHz: 868, TxPowerDBm: 22, TxAntennaGainDB: 3, RxSensitivityDB: -124, FadeMarginDB: 20, RxHeightM: 2, MarginGreenDB: 15}
	longitudeDegreesAt55N := 100 / (EarthRadiusKm * math.Cos(55*math.Pi/180)) * 180 / math.Pi
	fixtures := []struct {
		name    string
		bounds  Bounds
		sites   []Site
		rangeKm float64
	}{
		{name: "empty-area", bounds: Bounds{South: 50, North: 51, West: -2, East: -1}, rangeKm: 100},
		{name: "dense-urban-cluster", bounds: Bounds{South: 51.45, North: 51.65, West: -0.25, East: 0.05}, rangeKm: 100, sites: func() []Site {
			sites := make([]Site, 300)
			for i := range sites {
				sites[i] = Site{Lat: 51.5 + float64(i%20)*0.002, Lon: -0.2 + float64(i/20)*0.002, GroundM: 40, TxHeightM: 50}
			}
			return sites
		}()},
		{name: "national-border", bounds: Bounds{South: 54.9, North: 55.1, West: -6.2, East: -5.8}, rangeKm: 100, sites: []Site{
			{Lat: 54.99, Lon: -6.01, GroundM: 80, TxHeightM: 90},
			{Lat: 55.01, Lon: -5.99, GroundM: 70, TxHeightM: 80},
		}},
		{name: "high-latitude", bounds: Bounds{South: 79.8, North: 80.2, West: 8, East: 12}, rangeKm: 100, sites: []Site{
			{Lat: 80, Lon: 5.9, GroundM: 20, TxHeightM: 30},
			{Lat: 80, Lon: 14.1, GroundM: 20, TxHeightM: 30},
		}},
		{name: "maximum-range-boundary", bounds: Bounds{South: 54.99, North: 55.01, West: -3.01, East: -2.99}, rangeKm: 100, sites: []Site{
			{Lat: 55, Lon: -3 + longitudeDegreesAt55N, GroundM: 100, TxHeightM: 120},
			{Lat: 55, Lon: -3 - longitudeDegreesAt55N, GroundM: 100, TxHeightM: 120},
		}},
	}

	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			want := ComputeMarginsCPUReference(ukFlatGrid{elevation: 50}, fixture.sites, fixture.bounds, 31, 29, fixture.rangeKm, p, nil)
			got := ComputeMarginsCPU(ukFlatGrid{elevation: 50}, fixture.sites, fixture.bounds, 31, 29, fixture.rangeKm, p, nil)
			for i := range want {
				if math.IsNaN(float64(want[i])) && math.IsNaN(float64(got[i])) {
					continue
				}
				if math.Abs(float64(want[i]-got[i])) > 1e-5 {
					t.Fatalf("pixel %d differs: optimized=%v reference=%v", i, got[i], want[i])
				}
			}
		})
	}
}

func TestSpatialIndexReusesCandidateBuffer(t *testing.T) {
	sites := []Site{{Lat: 51, Lon: 0}, {Lat: 51.1, Lon: 0.1}}
	idx := NewSpatialIndex(sites, 100)
	buf := make([]int, 0, len(sites))
	first := idx.Query(51, 0, 100, buf)
	second := idx.Query(51.05, 0.05, 100, first)
	if cap(second) != cap(buf) || !slices.Equal(second, []int{0, 1}) {
		t.Fatalf("candidate buffer not reused as expected: %#v (cap %d, want %d)", second, cap(second), cap(buf))
	}
}
