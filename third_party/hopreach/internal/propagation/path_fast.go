// UK Mesh modifications, 2026-08-02.
//
// This file is derived integration work for HopReach and is licensed under
// the same AGPL-3.0 plus Commons-Clause terms as the upstream source.
package propagation

import "math"

// rasterParams contains link-budget terms which are invariant for every path
// in one raster. Keeping these outside the inner loop avoids repeating the
// same logarithm and wavelength division millions of times.
type rasterParams struct {
	wavelengthM        float64
	frequencyLossDB    float64
	receivedBeforeLoss float64
	rxHeightM          float64
}

func prepareRasterParams(p Params) rasterParams {
	return rasterParams{
		wavelengthM:        speedOfLight / (p.FrequencyMHz * 1e6),
		frequencyLossDB:    20*math.Log10(p.FrequencyMHz) + 32.44,
		receivedBeforeLoss: p.TxPowerDBm + p.TxAntennaGainDB + p.RxAntennaGainDB - p.RxSensitivityDB - p.FadeMarginDB,
		rxHeightM:          p.RxHeightM,
	}
}

type pathSampleGeometry struct {
	frac               float64
	fracProduct        float64
	invSqrtFracProduct float64
}

// TerrainProfileGrid is an optional acceleration interface. Implementations
// fill every requested terrain sample; the propagation package retains all RF
// physics and falls back to Grid.At for generic grids and test oracles.
type TerrainProfileGrid interface {
	SampleTerrainProfile(txLat, txLon, rxLat, rxLon float64, fractions, dst []float64)
}

// PathMargin uses 8..300 equally spaced samples. Their dimensionless
// positions and Fresnel geometry never change, so calculate them once for the
// process rather than once for every transmitter/pixel pair.
var pathSampleTables = func() [301][]pathSampleGeometry {
	var tables [301][]pathSampleGeometry
	for samples := 8; samples <= 300; samples++ {
		table := make([]pathSampleGeometry, samples-1)
		for i := 1; i < samples; i++ {
			frac := float64(i) / float64(samples)
			table[i-1] = pathSampleGeometry{
				frac:               frac,
				fracProduct:        frac * (1 - frac),
				invSqrtFracProduct: 1 / math.Sqrt(frac*(1-frac)),
			}
		}
		tables[samples] = table
	}
	return tables
}()

var pathSampleFractions = func() [301][]float64 {
	var tables [301][]float64
	for samples := 8; samples <= 300; samples++ {
		fractions := make([]float64, samples-1)
		for i := 1; i < samples; i++ {
			fractions[i-1] = float64(i) / float64(samples)
		}
		tables[samples] = fractions
	}
	return tables
}()

// pathMarginPrepared is algebraically identical to PathMargin, but factors
// the Fresnel term into one path-level scale and precomputed dimensionless
// sample geometry. PathMargin remains unchanged as the executable upstream
// oracle; parity tests bound the floating-point rearrangement below float32
// raster precision.
func pathMarginPrepared(grid Grid, profileGrid TerrainProfileGrid, terrainProfile []float64, rp rasterParams, txLat, txLon, txHeightM, rxLat, rxLon, rxHeightASL, distanceKm float64) float64 {
	distanceM := distanceKm * 1000
	samples := int(distanceKm / 0.05)
	if samples < 8 {
		samples = 8
	}
	if samples > 300 {
		samples = 300
	}

	// 1/d1 + 1/d2 = 1/(distanceM*frac*(1-frac)). This moves
	// the expensive square root out of the sample loop.
	fresnelScale := math.Sqrt((2 / rp.wavelengthM) / distanceM)
	curvatureScale := (distanceM * distanceM) / (2 * refractionK * earthRadiusM)
	latDelta := rxLat - txLat
	lonDelta := rxLon - txLon
	heightDelta := rxHeightASL - txHeightM
	table := pathSampleTables[samples]
	terrainProfile = terrainProfile[:len(table)]
	if profileGrid != nil {
		profileGrid.SampleTerrainProfile(txLat, txLon, rxLat, rxLon, pathSampleFractions[samples], terrainProfile)
	} else {
		for i, sample := range table {
			terrainProfile[i] = grid.At(txLat+latDelta*sample.frac, txLon+lonDelta*sample.frac)
		}
	}

	maxV := math.Inf(-1)
	for i, sample := range table {
		frac := sample.frac
		terrainM := terrainProfile[i]
		curvatureDropM := curvatureScale * sample.fracProduct
		effectiveTerrainM := terrainM - curvatureDropM
		directLineM := txHeightM + heightDelta*frac
		obstructionM := effectiveTerrainM - directLineM

		v := obstructionM * fresnelScale * sample.invSqrtFracProduct
		if v > maxV {
			maxV = v
		}
	}

	distanceForLoss := distanceKm
	if distanceForLoss < 0.001 {
		distanceForLoss = 0.001
	}
	loss := 20*math.Log10(distanceForLoss) + rp.frequencyLossDB
	if maxV > -0.78 {
		loss += KnifeEdgeDiffractionDB(maxV)
	}
	return rp.receivedBeforeLoss - loss
}
