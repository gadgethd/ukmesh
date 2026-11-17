// UK Mesh modifications. Licensed with HopReach under AGPL-3.0 plus Commons Clause.
package propagation

import (
	"math"
	"reflect"
	"testing"
)

func equivalenceInputBits(sites []Site) [][4]uint64 {
	bits := make([][4]uint64, len(sites))
	for i, site := range sites {
		bits[i] = [4]uint64{math.Float64bits(site.Lat), math.Float64bits(site.Lon), math.Float64bits(site.GroundM), math.Float64bits(site.TxHeightM)}
	}
	return bits
}

func TestCPUSiteEquivalenceRetainsFiniteBitDifferencesAndNonfiniteInputs(t *testing.T) {
	zero := Site{}
	input := []Site{zero, zero}
	want := []Site{zero}
	for field := 0; field < 4; field++ {
		for _, value := range []float64{math.Copysign(0, -1), math.SmallestNonzeroFloat64, 1} {
			site := zero
			fields := []*float64{&site.Lat, &site.Lon, &site.GroundM, &site.TxHeightM}
			*fields[field] = value
			input = append(input, site, site)
			want = append(want, site)
		}
		for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
			site := zero
			fields := []*float64{&site.Lat, &site.Lon, &site.GroundM, &site.TxHeightM}
			*fields[field] = value
			input = append(input, site, site)
			want = append(want, site, site)
		}
	}
	before := equivalenceInputBits(input)
	got := cpuSiteRepresentatives(input)
	if !reflect.DeepEqual(equivalenceInputBits(got), equivalenceInputBits(want)) {
		t.Fatal("stable full-bit representatives changed signed zeros, a height, a position, or a nonfinite occurrence")
	}
	if !reflect.DeepEqual(equivalenceInputBits(input), before) {
		t.Fatal("work projection mutated the complete identity/site input")
	}
	for _, empty := range [][]Site{nil, {}} {
		if len(cpuSiteRepresentatives(empty)) != 0 {
			t.Fatal("empty sites produced work")
		}
	}
}
