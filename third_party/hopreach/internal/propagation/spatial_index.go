// UK Mesh modifications, 2026-08-02.
//
// This file is derived integration work for HopReach and is licensed under
// the same AGPL-3.0 plus Commons-Clause terms as the vendored HopReach source.
package propagation

import (
	"math"
	"sort"
)

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
	siteCosLat  []float64
	bins        []spatialBin
	binMask     uint32
	siteIndices []int
}

type spatialBin struct {
	// keyPlusOne reserves zero as the empty-table sentinel.
	keyPlusOne uint32
	start      uint32
	end        uint32
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
		siteCosLat:  make([]float64, len(sites)),
		siteIndices: make([]int, len(sites)),
	}
	for siteIndex, site := range sites {
		idx.siteCosLat[siteIndex] = math.Cos(site.Lat * math.Pi / 180)
		idx.siteIndices[siteIndex] = siteIndex
	}
	sort.Slice(idx.siteIndices, func(i, j int) bool {
		left, right := idx.siteIndices[i], idx.siteIndices[j]
		leftKey := idx.keyForSite(sites[left])
		rightKey := idx.keyForSite(sites[right])
		if leftKey == rightKey {
			return left < right
		}
		return leftKey < rightKey
	})

	uniqueBins := 0
	previousKey := -1
	for _, siteIndex := range idx.siteIndices {
		key := idx.keyForSite(sites[siteIndex])
		if key != previousKey {
			uniqueBins++
			previousKey = key
		}
	}
	tableSize := 1
	for tableSize < uniqueBins*2 {
		tableSize <<= 1
	}
	idx.bins = make([]spatialBin, tableSize)
	idx.binMask = uint32(tableSize - 1)
	for start := 0; start < len(idx.siteIndices); {
		key := idx.keyForSite(sites[idx.siteIndices[start]])
		end := start + 1
		for end < len(idx.siteIndices) && idx.keyForSite(sites[idx.siteIndices[end]]) == key {
			end++
		}
		idx.insertBin(key, start, end)
		start = end
	}
	return idx
}

func (idx *SpatialIndex) keyForSite(site Site) int {
	return idx.latBin(site.Lat)*idx.lonBinCount + idx.lonBin(site.Lon)
}

func spatialBinHash(key uint32) uint32 {
	// Knuth's multiplicative hash disperses adjacent geographic bin keys.
	return key * 2654435761
}

func (idx *SpatialIndex) insertBin(key, start, end int) {
	position := spatialBinHash(uint32(key)) & idx.binMask
	for idx.bins[position].keyPlusOne != 0 {
		position = (position + 1) & idx.binMask
	}
	idx.bins[position] = spatialBin{keyPlusOne: uint32(key) + 1, start: uint32(start), end: uint32(end)}
}

func (idx *SpatialIndex) appendBin(dst []int, key int) []int {
	if len(idx.bins) == 0 {
		return dst
	}
	keyPlusOne := uint32(key) + 1
	position := spatialBinHash(uint32(key)) & idx.binMask
	for {
		bin := idx.bins[position]
		if bin.keyPlusOne == 0 {
			return dst
		}
		if bin.keyPlusOne == keyPlusOne {
			return append(dst, idx.siteIndices[bin.start:bin.end]...)
		}
		position = (position + 1) & idx.binMask
	}
}

func haversinePrepared(lat, lon, cosLat float64, site Site, siteCosLat float64) float64 {
	const radiansPerDegree = math.Pi / 180
	dLat := (site.Lat - lat) * radiansPerDegree
	dLon := (site.Lon - lon) * radiansPerDegree
	sinHalfLat := math.Sin(dLat / 2)
	sinHalfLon := math.Sin(dLon / 2)
	a := sinHalfLat*sinHalfLat + cosLat*siteCosLat*sinHalfLon*sinHalfLon
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return EarthRadiusKm * c
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
			dst = idx.appendBin(dst, key)
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
