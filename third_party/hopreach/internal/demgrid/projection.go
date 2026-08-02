// UK Mesh modifications, 2026-08-02.
//
// This file is derived integration work for HopReach and is licensed under
// the same AGPL-3.0 plus Commons-Clause terms as the upstream source.
package demgrid

import "math"

const latitudeProjectionStepDegrees = 0.001

type latitudeProjectionInterval struct {
	a float64
	b float64
	c float64
	d float64
}

// latitudeProjection is an immutable cubic-Hermite table for the latitude
// span of one tile-aligned DEM. Web-Mercator projection is sampled far more
// often than any other transcendental operation during terrain propagation;
// this table replaces a Log+Tan pair with bounded polynomial interpolation.
type latitudeProjection struct {
	minLatitude float64
	invStep     float64
	intervals   []latitudeProjectionInterval
}

func latitudeMercatorExact(latitude float64) float64 {
	latitudeRadians := latitude * math.Pi / 180
	return math.Log(math.Tan(math.Pi/4 + latitudeRadians/2))
}

func tileYToLatitude(tileY float64, zoom int) float64 {
	n := zoomScale(zoom)
	return math.Atan(math.Sinh(math.Pi*(1-2*tileY/n))) * 180 / math.Pi
}

func newLatitudeProjection(zoom, minTileY, tilesHigh int) *latitudeProjection {
	if tilesHigh <= 0 {
		return nil
	}
	minLatitude := tileYToLatitude(float64(minTileY+tilesHigh), zoom)
	maxLatitude := tileYToLatitude(float64(minTileY), zoom)
	entryCount := int(math.Ceil((maxLatitude-minLatitude)/latitudeProjectionStepDegrees)) + 1
	if entryCount < 2 {
		entryCount = 2
	}
	values := make([]float64, entryCount)
	slopes := make([]float64, entryCount)
	worldPixels := zoomScale(zoom) * tileSize
	minPixelY := float64(minTileY * tileSize)
	for i := range values {
		latitude := minLatitude + float64(i)*latitudeProjectionStepDegrees
		latitudeRadians := latitude * math.Pi / 180
		mercator := latitudeMercatorExact(latitude)
		values[i] = (1-mercator/math.Pi)/2*worldPixels - minPixelY
		slopes[i] = -worldPixels / (360 * math.Cos(latitudeRadians))
	}
	intervals := make([]latitudeProjectionInterval, entryCount-1)
	for i := range intervals {
		leftValue, rightValue := values[i], values[i+1]
		leftSlope := slopes[i] * latitudeProjectionStepDegrees
		rightSlope := slopes[i+1] * latitudeProjectionStepDegrees
		// Cubic-Hermite coefficients in Horner order for t in [0,1].
		intervals[i] = latitudeProjectionInterval{
			a: 2*leftValue - 2*rightValue + leftSlope + rightSlope,
			b: -3*leftValue + 3*rightValue - 2*leftSlope - rightSlope,
			c: leftSlope,
			d: leftValue,
		}
	}
	return &latitudeProjection{
		minLatitude: minLatitude,
		invStep:     1 / latitudeProjectionStepDegrees,
		intervals:   intervals,
	}
}

func (p *latitudeProjection) pixelY(latitude float64) (float64, bool) {
	if p == nil {
		return 0, false
	}
	position := (latitude - p.minLatitude) * p.invStep
	index := int(position)
	if position < 0 || index >= len(p.intervals) {
		return 0, false
	}
	t := position - float64(index)
	interval := p.intervals[index]
	return ((interval.a*t+interval.b)*t+interval.c)*t + interval.d, true
}
