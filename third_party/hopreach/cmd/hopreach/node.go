package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"hopreach/internal/compute"
	"hopreach/internal/corescope"
	"hopreach/internal/coverage"
	"hopreach/internal/geo"
	"hopreach/internal/propagation"
)

var nodeDatasetPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

const (
	nodeCoverageNamespace  = "nodes"
	nodeCoverageMaxEntries = 128
	nodeCoverageRetention  = 7 * 24 * time.Hour
)

type nodeRunResult struct {
	PublicKey string `json:"public_key"`
	State     string `json:"state"`
	DatasetID string `json:"dataset_id,omitempty"`
	Cached    bool   `json:"cached,omitempty"`
	Message   string `json:"message,omitempty"`
}

func normalizeNodePublicKey(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 64 {
		return "", fmt.Errorf("node public key must contain exactly 64 hexadecimal characters")
	}
	if _, err := hex.DecodeString(value); err != nil {
		return "", fmt.Errorf("node public key must contain exactly 64 hexadecimal characters")
	}
	return value, nil
}

func nodeDatasetID(publicKey string, node corescope.Node, cfg appConfig) (string, error) {
	identity := struct {
		PublicKey   string             `json:"public_key"`
		Lat         float64            `json:"lat"`
		Lon         float64            `json:"lon"`
		LastHeard   *string            `json:"last_heard"`
		DEMZoom     int                `json:"dem_zoom"`
		Width       int                `json:"width"`
		TileSize    int                `json:"tile_size"`
		MaxAlpha    uint8              `json:"max_alpha"`
		Propagation propagation.Params `json:"propagation"`
	}{publicKey, *node.Lat, *node.Lon, node.LastHeard, cfg.demZoom, cfg.coverageImageWidth,
		cfg.coveragePublicationTileSize, cfg.coverageMaxAlpha, cfg.propagation}
	data, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return "n" + hex.EncodeToString(digest[:12]), nil
}

func readNodeCoverageIndex(outputDir string) (map[string]*nodeCoverageMeta, error) {
	data, err := os.ReadFile(filepath.Join(outputDir, "meta.json"))
	if err != nil {
		return nil, fmt.Errorf("reading existing meta.json: %w", err)
	}
	var holder struct {
		NodeCoverage map[string]*nodeCoverageMeta `json:"node_coverage"`
	}
	if err := json.Unmarshal(data, &holder); err != nil {
		return nil, fmt.Errorf("decoding existing meta.json: %w", err)
	}
	if holder.NodeCoverage == nil {
		holder.NodeCoverage = make(map[string]*nodeCoverageMeta)
	}
	return holder.NodeCoverage, nil
}

// writeNodeCoverageIndex replaces only meta.json's node_coverage member. Raw
// messages preserve global coverage and meta.run byte-for-byte semantically;
// a node job never owns or updates those fields.
func writeNodeCoverageIndex(outputDir string, index map[string]*nodeCoverageMeta) error {
	path := filepath.Join(outputDir, "meta.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading existing meta.json: %w", err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("decoding existing meta.json: %w", err)
	}
	nodes, err := json.Marshal(index)
	if err != nil {
		return err
	}
	document["node_coverage"] = nodes
	return writeJSONFile(path, document)
}

func nodeCoverageFresh(entry *nodeCoverageMeta, node corescope.Node, cfg appConfig, now time.Time) bool {
	if entry == nil || entry.State != "available" || entry.Standard == nil || len(entry.Standard.Tiles) == 0 {
		return false
	}
	generated, err := time.Parse(time.RFC3339, entry.Standard.GeneratedAt)
	if err != nil || now.Sub(generated) >= time.Duration(cfg.minRecomputeIntervalHours*float64(time.Hour)) {
		return false
	}
	// A newer advert timestamp alone does not invalidate a six-hour raster;
	// coordinates or the active/degraded class changing does. LastHeard is
	// still recorded and becomes part of the next dataset identity once the
	// configured freshness window expires.
	return entry.Lat == *node.Lat && entry.Lon == *node.Lon &&
		entry.PositionStatus == classifyStatus(node.LastHeard, cfg)
}

func pruneNodeCoverage(index map[string]*nodeCoverageMeta, preserve string, now time.Time) {
	cutoff := now.Add(-nodeCoverageRetention)
	for key, entry := range index {
		if key == preserve || entry == nil {
			continue
		}
		updated, err := time.Parse(time.RFC3339, entry.UpdatedAt)
		if err != nil || updated.Before(cutoff) {
			delete(index, key)
		}
	}
	if len(index) <= nodeCoverageMaxEntries {
		return
	}
	type candidate struct {
		key     string
		updated time.Time
	}
	candidates := make([]candidate, 0, len(index))
	for key, entry := range index {
		if key == preserve {
			continue
		}
		updated, _ := time.Parse(time.RFC3339, entry.UpdatedAt)
		candidates = append(candidates, candidate{key, updated})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].updated.Before(candidates[j].updated) })
	for _, candidate := range candidates {
		if len(index) <= nodeCoverageMaxEntries {
			break
		}
		delete(index, candidate.key)
	}
}

func cleanupUnreferencedNodeDatasets(outputDir string, index map[string]*nodeCoverageMeta) {
	referenced := make(map[string]bool)
	for _, entry := range index {
		if entry == nil || entry.Standard == nil {
			continue
		}
		for _, tile := range entry.Standard.Tiles {
			parts := strings.Split(filepath.ToSlash(tile.Image), "/")
			if len(parts) == 4 && parts[0] == "tiles" && parts[1] == nodeCoverageNamespace && nodeDatasetPattern.MatchString(parts[2]) {
				referenced[parts[2]] = true
			}
		}
	}
	tileRoot := filepath.Join(outputDir, "tiles", nodeCoverageNamespace)
	entries, err := os.ReadDir(tileRoot)
	if err != nil {
		return
	}
	for _, entry := range entries {
		id := entry.Name()
		if !entry.IsDir() || referenced[id] || !nodeDatasetPattern.MatchString(id) {
			continue
		}
		_ = os.RemoveAll(filepath.Join(tileRoot, id))
		_ = os.Remove(filepath.Join(outputDir, "checkpoints", nodeCoverageNamespace, id+".json"))
	}
}

func nodeTilesExist(outputDir string, entry *nodeCoverageMeta) bool {
	if entry == nil || entry.Standard == nil || len(entry.Standard.Tiles) == 0 {
		return false
	}
	prefix := "tiles/" + nodeCoverageNamespace + "/" + entry.DatasetID + "/"
	for _, tile := range entry.Standard.Tiles {
		image := filepath.ToSlash(tile.Image)
		if !strings.HasPrefix(image, prefix) || strings.Contains(image, "..") {
			return false
		}
		info, err := os.Stat(filepath.Join(outputDir, filepath.FromSlash(image)))
		if err != nil || info.Size() == 0 {
			return false
		}
	}
	return true
}

func runNode(cfg appConfig, publicKey string) (result nodeRunResult, err error) {
	result.PublicKey = publicKey
	now := time.Now().UTC()
	httpClient := &http.Client{Timeout: cfg.timeout}
	boundaryCacheDir := filepath.Join(cfg.demCacheDir, "boundary")
	region, err := geo.LoadBoundary(cfg.regionBoundaryPath, cfg.regionBoundaryURL, boundaryCacheDir, httpClient)
	if err != nil {
		return result, err
	}
	nodes, err := corescope.NewClient(cfg.apiURL, httpClient).FetchRepeaters(context.Background())
	if err != nil {
		return result, err
	}
	var node *corescope.Node
	for i := range nodes {
		if strings.EqualFold(nodes[i].PublicKey, publicKey) {
			node = &nodes[i]
			break
		}
	}
	if node == nil {
		result.State, result.Message = "not_found", "repeater was not returned by CoreScope"
		return result, nil
	}
	if node.Lat == nil || node.Lon == nil || math.IsNaN(*node.Lat) || math.IsNaN(*node.Lon) || math.IsInf(*node.Lat, 0) || math.IsInf(*node.Lon, 0) || *node.Lat < -90 || *node.Lat > 90 || *node.Lon < -180 || *node.Lon > 180 {
		result.State, result.Message = "invalid_coordinates", "repeater has no valid coordinates"
		return result, nil
	}
	if !region.Contains(*node.Lat, *node.Lon) {
		result.State, result.Message = "out_of_region", "repeater is outside the configured region"
		return result, nil
	}
	if cfg.requiredScope != "" && (node.DefaultScope == nil || strings.TrimPrefix(*node.DefaultScope, "#") != cfg.requiredScope) {
		result.State, result.Message = "out_of_region", "repeater is outside the configured scope"
		return result, nil
	}
	positionStatus := classifyStatus(node.LastHeard, cfg)
	index, err := readNodeCoverageIndex(cfg.outputDir)
	if err != nil {
		return result, err
	}
	if positionStatus == "silent" {
		if existing := index[publicKey]; existing != nil {
			existing.PositionStatus = "silent"
			existing.FreshUntil = now.Format(time.RFC3339)
			existing.UpdatedAt = now.Format(time.RFC3339)
			_ = writeNodeCoverageIndex(cfg.outputDir, index)
		}
		result.State, result.Message = "stale", "repeater position is too old for current RF coverage"
		return result, nil
	}

	if existing := index[publicKey]; nodeCoverageFresh(existing, *node, cfg, now) && nodeTilesExist(cfg.outputDir, existing) {
		result.State, result.DatasetID, result.Cached = "available", existing.DatasetID, true
		return result, nil
	}
	datasetID, err := nodeDatasetID(publicKey, *node, cfg)
	if err != nil {
		return result, err
	}
	for key, entry := range index {
		if key != publicKey && entry != nil && entry.DatasetID == datasetID {
			return result, fmt.Errorf("node dataset ID collision between %s and %s", publicKey, key)
		}
	}
	result.DatasetID = datasetID
	previous := index[publicKey]
	entry := &nodeCoverageMeta{
		DatasetID: datasetID, RunID: "node-" + datasetID, State: "computing", PositionStatus: positionStatus,
		LastHeard: node.LastHeard, Lat: *node.Lat, Lon: *node.Lon,
		RequestedAt: now.Format(time.RFC3339), UpdatedAt: now.Format(time.RFC3339),
	}
	if previous != nil {
		entry.Standard = previous.Standard
	}
	index[publicKey] = entry
	pruneNodeCoverage(index, publicKey, now)
	if err := writeNodeCoverageIndex(cfg.outputDir, index); err != nil {
		return result, err
	}

	defer func() {
		if err == nil {
			return
		}
		entry.State, entry.Failure, entry.UpdatedAt = "failed", err.Error(), time.Now().UTC().Format(time.RFC3339)
		_ = writeNodeCoverageIndex(cfg.outputDir, index)
		result.State, result.Message = "failed", err.Error()
	}()

	rangeKm := propagation.LinkBudgetMaxRangeKm(cfg.propagation)
	point := coverage.Point{Lat: *node.Lat, Lon: *node.Lon}
	bounds, ok := coverage.RasterBounds([]coverage.Point{point}, rangeKm)
	if !ok {
		return result, fmt.Errorf("could not calculate node raster bounds")
	}
	grounds, err := loadSiteGrounds([]coverage.Point{point}, cfg.demZoom, cfg.demCacheDir, cfg.demTileURLBase, httpClient, nil)
	if err != nil {
		return result, err
	}
	site := propagation.Site{Lat: point.Lat, Lon: point.Lon, GroundM: grounds[0], TxHeightM: grounds[0] + cfg.propagation.AntennaHeightM}
	engine := compute.New()
	engine.Setup(cfg.gpuMode)
	engine.SetRemote(cfg.gpuBrokerAddr, cfg.demTileURLBase)
	if cfg.coveragePrecisionChunkBudgetMB > 0 {
		engine.SetChunkBudgetBytes(float64(cfg.coveragePrecisionChunkBudgetMB) * 1_000_000)
	}
	tiles, err := coverage.RasterProgressiveChunked(
		engine, bounds, cfg.demZoom, cfg.demCacheDir, cfg.demTileURLBase, httpClient,
		[]propagation.Site{site}, cfg.coverageImageWidth, 1, cfg.propagation, cfg.coverageMaxAlpha,
		coverage.ProgressiveOptions{
			OutputDir: cfg.outputDir, Namespace: nodeCoverageNamespace, Tier: datasetID,
			RunID: entry.RunID, Identity: publicKey, TileSize: cfg.coveragePublicationTileSize,
			OnTile: func(completed, total int, _ []coverage.Tile) error {
				entry.CompletedTiles, entry.TotalTiles = completed, total
				entry.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
				return writeNodeCoverageIndex(cfg.outputDir, index)
			},
		}, nil,
	)
	if err != nil {
		return result, err
	}
	note := fmt.Sprintf("Standard-tier coverage from this repeater alone (%s position); not its contribution to the merged best-server network raster.", positionStatus)
	standard := buildCoverageMeta(tiles, rangeKm, cfg, note)
	finished := time.Now().UTC()
	entry.Standard = &standard
	entry.State, entry.Failure = "available", ""
	entry.CompletedTiles, entry.TotalTiles = len(tiles), len(tiles)
	entry.UpdatedAt = finished.Format(time.RFC3339)
	entry.FreshUntil = finished.Add(time.Duration(cfg.minRecomputeIntervalHours * float64(time.Hour))).Format(time.RFC3339)
	if err := writeNodeCoverageIndex(cfg.outputDir, index); err != nil {
		return result, err
	}
	cleanupUnreferencedNodeDatasets(cfg.outputDir, index)
	result.State = "available"
	return result, nil
}
