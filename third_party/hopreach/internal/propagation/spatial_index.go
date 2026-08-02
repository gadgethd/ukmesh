// UK Mesh modifications, 2026-08-02.
//
// This file is derived integration work for HopReach and is licensed under
// the same AGPL-3.0 plus Commons-Clause terms as the vendored HopReach source.
package propagation

import "math"

// SpatialIndex bins transmitter coordinates in latitude/longitude space.
// Query first returns a conservative geographic superset; callers must retain
// HopReach's exact Haversine cutoff before evaluating a path.
//
// The index contains only coordinate lookups. It deliberately does not change
// propagation parameters, terrain sampling, or any part of PathMargin.
type SpatialIndex struct {
	sites       []Site
	binDegrees  float64
	latBinCount int
	lonBinCount int
	bins        map[int][]int
}

const (
	minSpatialBinDegrees = 0.10
	maxSpatialBinDegrees = 1.00
)

// NewSpatialIndex precomputes reusable site coordinate bins for one raster.
func NewSpatialIndex(sites []Site, rangeKm float64) *SpatialIndex {
	angularDegrees := rangeKm / EarthRadiusKm * 180 / math.Pi
	binDegrees := angularDegrees / 2
	if binDegrees < minSpatialBinDegrees {
		binDegrees = minSpatialBinDegrees
	}
	if binDegrees > maxSpatialBinDegrees {
		binDegrees = maxSpatialBinDegrees
	}
	latBins := int(math.Ceil(180 / binDegrees))
	lonBins := int(math.Ceil(360 / binDegrees))
	idx := &SpatialIndex{
		sites:       sites,
		binDegrees:  binDegrees,
		latBinCount: latBins,
		lonBinCount: lonBins,
		bins:        make(map[int][]int, len(sites)),
	}
	for siteIndex, site := range sites {
		latBin := idx.latBin(site.Lat)
		lonBin := idx.lonBin(site.Lon)
		key := latBin*idx.lonBinCount + lonBin
		idx.bins[key] = append(idx.bins[key], siteIndex)
	}
	return idx
}

func (idx *SpatialIndex) latBin(lat float64) int {
	lat = math.Max(-90, math.Min(90, lat))
	bin := int(math.Floor((lat + 90) / idx.binDegrees))
	if bin >= idx.latBinCount {
		return idx.latBinCount - 1
	}
	return bin
}

func normalizeLongitude(lon float64) float64 {
	lon = math.Mod(lon+180, 360)
	if lon < 0 {
		lon += 360
	}
	return lon - 180
}

func (idx *SpatialIndex) lonBin(lon float64) int {
	lon = normalizeLongitude(lon)
	bin := int(math.Floor((lon + 180) / idx.binDegrees))
	if bin >= idx.lonBinCount {
		return idx.lonBinCount - 1
	}
	return bin
}

// longitudePaddingDegrees returns a conservative longitude half-width for a
// spherical cap of angularRadius centred on lat. It uses the exact haversine
// inequality and the smallest cosine reachable within the latitude interval.
// If the cap reaches a pole, every longitude is conservatively selected.
func longitudePaddingDegrees(lat, angularRadius float64) float64 {
	if angularRadius >= math.Pi || math.Abs(lat)+angularRadius >= math.Pi/2 {
		return 180
	}
	maxAbsLat := math.Abs(lat) + angularRadius
	denominator := math.Sqrt(math.Cos(lat) * math.Cos(maxAbsLat))
	if denominator <= 0 {
		return 180
	}
	ratio := math.Sin(angularRadius/2) / denominator
	if ratio >= 1 {
		return 180
	}
	return 2 * math.Asin(ratio) * 180 / math.Pi
}

// Query appends the indices of every site which might be within rangeKm of
// lat/lon to dst and returns the resulting slice. dst is reset and reused, so
// one buffer per raster worker avoids per-pixel allocations.
func (idx *SpatialIndex) Query(lat, lon, rangeKm float64, dst []int) []int {
	dst = dst[:0]
	if idx == nil || len(idx.sites) == 0 || rangeKm < 0 {
		return dst
	}

	angularRadius := rangeKm / EarthRadiusKm
	latPad := angularRadius * 180 / math.Pi
	minLat := math.Max(-90, lat-latPad)
	maxLat := math.Min(90, lat+latPad)
	minLatBin, maxLatBin := idx.latBin(minLat), idx.latBin(maxLat)
	lonPad := longitudePaddingDegrees(lat*math.Pi/180, angularRadius)

	appendLonBins := func(latBin, first, last int) {
		for lonBin := first; lonBin <= last; lonBin++ {
			key := latBin*idx.lonBinCount + lonBin
			dst = append(dst, idx.bins[key]...)
		}
	}

	for latBin := minLatBin; latBin <= maxLatBin; latBin++ {
		if lonPad >= 180 {
			appendLonBins(latBin, 0, idx.lonBinCount-1)
			continue
		}
		west := normalizeLongitude(lon - lonPad)
		east := normalizeLongitude(lon + lonPad)
		westBin, eastBin := idx.lonBin(west), idx.lonBin(east)
		if west <= east {
			appendLonBins(latBin, westBin, eastBin)
		} else {
			appendLonBins(latBin, westBin, idx.lonBinCount-1)
			appendLonBins(latBin, 0, eastBin)
		}
	}
	return dst
}
