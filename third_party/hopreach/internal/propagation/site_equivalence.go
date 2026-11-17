// UK Mesh modifications. Licensed with HopReach under AGPL-3.0 plus Commons Clause.
package propagation

import "math"

// cpuSiteRepresentatives leaves identity/publication inputs untouched. Coverage
// takes a maximum of independent margins, so an identical finite RF path only
// needs evaluating once. Equality includes resolved heights, not just position.
func cpuSiteRepresentatives(sites []Site) []Site {
	seen := make(map[[4]uint64]struct{}, len(sites))
	work := make([]Site, 0, len(sites))
	for _, site := range sites {
		values := [4]float64{site.Lat, site.Lon, site.GroundM, site.TxHeightM}
		var key [4]uint64
		finite := true
		for i, value := range values {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				finite = false
				break
			}
			key[i] = math.Float64bits(value)
		}
		if finite {
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
		}
		work = append(work, site)
	}
	return work
}
