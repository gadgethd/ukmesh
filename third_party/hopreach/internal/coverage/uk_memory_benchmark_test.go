// UK Mesh release-gate working-set measurements. These use demgrid.Load's
// exact Web-Mercator integer tile accounting (256*256 float32 per tile), so
// the reported MiB values are the real terrain-grid bytes each path maps.
package coverage

import (
	"math"
	"testing"

	"hopreach/internal/propagation"
)

const demTileWorkingBytes = 256 * 256 * 4

var ukOperationalV1Bounds = propagation.Bounds{
	// Exact bbox of rf-coverage/boundaries/uk-operational-v1.geojson. The
	// western extent includes Rockall in Natural Earth's GBR geometry; using
	// a mainland-only box would understate the former monolithic grid.
	South: 49.171332, North: 60.847886, West: -13.691314, East: 1.771169,
}

func demGridBytes(bounds propagation.Bounds, zoom int) int64 {
	n := math.Exp2(float64(zoom))
	tileX := func(lon float64) float64 { return (lon + 180) / 360 * n }
	tileY := func(lat float64) float64 {
		latRad := lat * math.Pi / 180
		return (1 - math.Asinh(math.Tan(latRad))/math.Pi) / 2 * n
	}
	minX, maxX := int(math.Floor(tileX(bounds.West))), int(math.Floor(tileX(bounds.East)))
	minY, maxY := int(math.Floor(tileY(bounds.North))), int(math.Floor(tileY(bounds.South)))
	return int64(maxX-minX+1) * int64(maxY-minY+1) * demTileWorkingBytes
}

func paddedForRange(bounds propagation.Bounds, rangeKm float64) propagation.Bounds {
	angular := rangeKm / propagation.EarthRadiusKm
	latPad := angular * 180 / math.Pi
	maxAbsLat := math.Max(math.Abs(bounds.South), math.Abs(bounds.North)) * math.Pi / 180
	lonPad := 180.0
	if angular < math.Pi && maxAbsLat+angular < math.Pi/2 {
		denominator := math.Sqrt(math.Cos(maxAbsLat) * math.Cos(maxAbsLat+angular))
		ratio := math.Sin(angular/2) / denominator
		if denominator > 0 && ratio < 1 {
			lonPad = 2 * math.Asin(ratio) * 180 / math.Pi
		}
	}
	return propagation.Bounds{
		South: math.Max(-90, bounds.South-latPad),
		North: math.Min(90, bounds.North+latPad),
		West:  bounds.West - lonPad,
		East:  bounds.East + lonPad,
	}
}

func progressivePeakDEMBytes(width, zoom int, rangeKm float64) int64 {
	w, h := dimensions(ukOperationalV1Bounds, width)
	var peak int64
	for _, tile := range planPublicationTiles(ukOperationalV1Bounds, w, h, 1024) {
		bytes := demGridBytes(paddedForRange(tile.bounds, rangeKm), zoom)
		if bytes > peak {
			peak = bytes
		}
	}
	return peak
}

func TestUKProgressiveTerrainWorkingSetReleaseGate(t *testing.T) {
	for _, tier := range []struct {
		name, model string
		width, zoom int
	}{
		{name: "standard", model: "2000px/z11", width: 2000, zoom: 11},
		{name: "precision", model: "6000px/z13/2x", width: 6000, zoom: 13},
	} {
		t.Run(tier.name, func(t *testing.T) {
			upstream := demGridBytes(ukOperationalV1Bounds, tier.zoom)
			optimized := progressivePeakDEMBytes(tier.width, tier.zoom, 100)
			// "Material" is pinned at a >=30% reduction. Padding every tile by
			// the full unchanged propagation range is included in optimized.
			if optimized >= upstream*70/100 {
				t.Fatalf("%s terrain peak = %.1f MiB, upstream %.1f MiB; want at least 30%% lower", tier.model, float64(optimized)/(1<<20), float64(upstream)/(1<<20))
			}
		})
	}
}

func BenchmarkUKTerrainWorkingSet(b *testing.B) {
	for _, tier := range []struct {
		name        string
		width, zoom int
	}{
		{name: "Standard", width: 2000, zoom: 11},
		{name: "Precision", width: 6000, zoom: 13},
	} {
		upstream := demGridBytes(ukOperationalV1Bounds, tier.zoom)
		optimized := progressivePeakDEMBytes(tier.width, tier.zoom, 100)
		b.Run("UpstreamMonolithic/"+tier.name, func(b *testing.B) {
			b.ReportMetric(float64(upstream)/(1<<20), "peak_DEM_MiB")
		})
		b.Run("OptimizedProgressive/"+tier.name, func(b *testing.B) {
			b.ReportMetric(float64(optimized)/(1<<20), "peak_DEM_MiB")
		})
	}
}
