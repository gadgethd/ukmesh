// UK Mesh modifications, 2026-08-02. Licensed with HopReach under
// AGPL-3.0 plus Commons Clause.
package demgrid

import (
	"math"
	"testing"
)

var projectionSink float64

func projectionOriginal(lat float64) float64 {
	latRad := lat * math.Pi / 180
	return math.Asinh(math.Tan(latRad))
}

func BenchmarkLatitudeProjection(b *testing.B) {
	latitudes := make([]float64, 4096)
	for i := range latitudes {
		latitudes[i] = 49 + float64(i)/float64(len(latitudes)-1)*13
	}
	b.Run("UpstreamAsinhTan", func(b *testing.B) {
		var sum float64
		for i := 0; i < b.N; i++ {
			for _, latitude := range latitudes {
				sum += projectionOriginal(latitude)
			}
		}
		projectionSink = sum
	})
	b.Run("TabulatedPixelY", func(b *testing.B) {
		const zoom = 13
		minTileY := int(math.Floor(latToTileY(62, zoom)))
		maxTileY := int(math.Floor(latToTileY(49, zoom)))
		projection := newLatitudeProjection(zoom, minTileY, maxTileY-minTileY+1)
		var sum float64
		for i := 0; i < b.N; i++ {
			for _, latitude := range latitudes {
				pixelY, ok := projection.pixelY(latitude)
				if !ok {
					b.Fatalf("projection does not cover latitude %.6f", latitude)
				}
				sum += pixelY
			}
		}
		projectionSink = sum
	})
}

func TestLatitudeProjectionAccuracy(t *testing.T) {
	for i := 0; i <= 170000; i++ {
		latitude := -85 + float64(i)/1000
		want := (1 - projectionOriginal(latitude)/math.Pi) / 2 * zoomScale(13)
		if got := latToTileY(latitude, 13); math.Abs(got-want) > 1e-11 {
			t.Fatalf("latToTileY(%.3f) = %.15g, want %.15g", latitude, got, want)
		}
	}

	const zoom = 13
	minTileY := int(math.Floor(latToTileY(62, zoom)))
	maxTileY := int(math.Floor(latToTileY(49, zoom)))
	production := newLatitudeProjection(zoom, minTileY, maxTileY-minTileY+1)
	worldPixels := zoomScale(zoom) * tileSize
	var maxProduction float64
	for i := 0; i <= 130000; i++ {
		latitude := 49 + float64(i)/10000
		want := projectionOriginal(latitude)
		got, ok := production.pixelY(latitude)
		if !ok {
			t.Fatalf("production projection does not cover latitude %.6f", latitude)
		}
		wantPixelY := (1-want/math.Pi)/2*worldPixels - float64(minTileY*tileSize)
		maxProduction = math.Max(maxProduction, math.Abs(got-wantPixelY))
	}
	if maxProduction > 1e-8 {
		t.Fatalf("production projection error %g pixels exceeds accuracy gate", maxProduction)
	}
}
