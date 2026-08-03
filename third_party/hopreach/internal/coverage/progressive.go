// UK Mesh modifications, 2026-08-02.
//
// Progressive, resumable publication is derived HopReach integration work and
// is licensed under HopReach's AGPL-3.0 plus Commons-Clause terms.
package coverage

import (
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
	"time"

	"hopreach/internal/compute"
	"hopreach/internal/propagation"
)

var tierNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
var namespacePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

// ProgressiveOptions controls resumable browser-tile publication. TileSize is
// served pixels, not the (optionally supersampled) compute resolution.
type ProgressiveOptions struct {
	OutputDir string
	Tier      string
	RunID     string
	TileSize  int
	// Identity is optional caller-owned input folded into the checkpoint
	// signature. Node jobs use the full public key here even though their
	// bounded filesystem tier uses only a short digest.
	Identity string
	// Namespace optionally places both tiles and checkpoints below one
	// additional, validated path segment. Global tiers leave this empty;
	// on-demand node jobs use "nodes" so they cannot collide with the
	// standard/precision publication or checkpoint namespaces.
	Namespace string
	// OnTile is called after each PNG and its checkpoint are safely written.
	// The tile list contains only tiles completed by this run so far.
	OnTile func(completed, total int, tiles []Tile) error
}

type progressiveCheckpoint struct {
	RunID      string          `json:"run_id"`
	Tier       string          `json:"tier"`
	Signature  string          `json:"signature"`
	StartedAt  string          `json:"started_at"`
	UpdatedAt  string          `json:"updated_at"`
	Completed  map[string]Tile `json:"completed"`
	TotalTiles int             `json:"total_tiles"`
	Complete   bool            `json:"complete"`
}

type progressiveTile struct {
	row, col int
	x0, x1   int
	y0, y1   int
	bounds   propagation.Bounds
	name     string
}

func planPublicationTiles(bounds propagation.Bounds, width, height, tileSize int) []progressiveTile {
	if tileSize < 1 {
		tileSize = 1024
	}
	tiles := make([]progressiveTile, 0, ((width+tileSize-1)/tileSize)*((height+tileSize-1)/tileSize))
	for y0, row := 0, 0; y0 < height; y0, row = y0+tileSize, row+1 {
		y1 := y0 + tileSize
		if y1 > height {
			y1 = height
		}
		for x0, col := 0, 0; x0 < width; x0, col = x0+tileSize, col+1 {
			x1 := x0 + tileSize
			if x1 > width {
				x1 = width
			}
			tileBounds := propagation.Bounds{
				North: bounds.North - float64(y0)/float64(height)*(bounds.North-bounds.South),
				South: bounds.North - float64(y1)/float64(height)*(bounds.North-bounds.South),
				West:  bounds.West + float64(x0)/float64(width)*(bounds.East-bounds.West),
				East:  bounds.West + float64(x1)/float64(width)*(bounds.East-bounds.West),
			}
			tiles = append(tiles, progressiveTile{
				row: row, col: col, x0: x0, x1: x1, y0: y0, y1: y1,
				bounds: tileBounds,
				name:   fmt.Sprintf("%d-%d.png", row, col),
			})
		}
	}
	return tiles
}

func progressiveSignature(bounds propagation.Bounds, zoom, width, height, supersample int, sites []propagation.Site, p propagation.Params) (string, error) {
	data, err := json.Marshal(struct {
		Model       string             `json:"model"`
		Bounds      propagation.Bounds `json:"bounds"`
		Zoom        int                `json:"zoom"`
		Width       int                `json:"width"`
		Height      int                `json:"height"`
		Supersample int                `json:"supersample"`
		Sites       []propagation.Site `json:"sites"`
		Params      propagation.Params `json:"params"`
	}{"hopreach-v0.1.32", bounds, zoom, width, height, supersample, sites, p})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func readProgressiveCheckpoint(path string) (*progressiveCheckpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var checkpoint progressiveCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func writeProgressiveCheckpoint(path string, checkpoint *progressiveCheckpoint) error {
	checkpoint.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return writeJSONAtomic(path, checkpoint)
}

func resumeProgressiveCheckpoint(
	checkpoint *progressiveCheckpoint,
	runID, tier, signature string,
	totalTiles int,
	now time.Time,
) (*progressiveCheckpoint, bool) {
	if checkpoint == nil || checkpoint.RunID != runID || checkpoint.Tier != tier ||
		checkpoint.Signature != signature || checkpoint.Complete {
		stamp := now.UTC().Format(time.RFC3339)
		return &progressiveCheckpoint{
			RunID: runID, Tier: tier, Signature: signature,
			StartedAt: stamp, UpdatedAt: stamp, Completed: map[string]Tile{},
			TotalTiles: totalTiles,
		}, true
	}
	if checkpoint.Completed == nil {
		checkpoint.Completed = map[string]Tile{}
	}
	checkpoint.TotalTiles = totalTiles
	return checkpoint, false
}

func writeJSONAtomic(path string, value any) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(value); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func checkpointTiles(checkpoint *progressiveCheckpoint) []Tile {
	names := make([]string, 0, len(checkpoint.Completed))
	for name := range checkpoint.Completed {
		names = append(names, name)
	}
	sort.Strings(names)
	tiles := make([]Tile, 0, len(names))
	for _, name := range names {
		tiles = append(tiles, checkpoint.Completed[name])
	}
	return tiles
}

// RasterProgressiveChunked computes one tier as independently publishable
// geographic tiles. Every tile includes all transmitters within the unchanged
// HopReach link-budget range, is atomically published as soon as complete, and
// is checkpointed so a restart can safely resume without blanking valid output.
func RasterProgressiveChunked(
	engine *compute.Engine,
	bounds propagation.Bounds,
	zoom int,
	cacheDir, tileURLBase string,
	client *http.Client,
	sites []propagation.Site,
	servedWidth, supersample int,
	p propagation.Params,
	maxAlpha uint8,
	options ProgressiveOptions,
	progress func(done, total int),
) ([]Tile, error) {
	if !tierNamePattern.MatchString(options.Tier) {
		return nil, fmt.Errorf("invalid progressive tier name %q", options.Tier)
	}
	if options.Namespace != "" && !namespacePattern.MatchString(options.Namespace) {
		return nil, fmt.Errorf("invalid progressive namespace %q", options.Namespace)
	}
	if options.RunID == "" {
		return nil, fmt.Errorf("progressive run ID is required")
	}
	servedW, servedH := dimensions(bounds, servedWidth)
	if servedW == 0 {
		return nil, nil
	}
	if supersample < 1 {
		supersample = 1
	}
	if options.TileSize < 1 {
		options.TileSize = 1024
	}
	publicationTiles := planPublicationTiles(bounds, servedW, servedH, options.TileSize)
	signature, err := progressiveSignature(bounds, zoom, servedW, servedH, supersample, sites, p)
	if err != nil {
		return nil, fmt.Errorf("progressive signature: %w", err)
	}
	if options.Identity != "" {
		digest := sha256.Sum256([]byte(signature + "\x00" + options.Identity))
		signature = hex.EncodeToString(digest[:])
	}

	tileParts := []string{options.OutputDir, "tiles"}
	checkpointParts := []string{options.OutputDir, "checkpoints"}
	publicParts := []string{"tiles"}
	if options.Namespace != "" {
		tileParts = append(tileParts, options.Namespace)
		checkpointParts = append(checkpointParts, options.Namespace)
		publicParts = append(publicParts, options.Namespace)
	}
	tileDir := filepath.Join(append(tileParts, options.Tier)...)
	checkpointDir := filepath.Join(checkpointParts...)
	publicParts = append(publicParts, options.Tier)
	if err := os.MkdirAll(tileDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(checkpointDir, 0o755); err != nil {
		return nil, err
	}
	checkpointPath := filepath.Join(checkpointDir, options.Tier+".json")
	checkpoint, readErr := readProgressiveCheckpoint(checkpointPath)
	if readErr != nil {
		// A corrupt checkpoint must never suppress real work. Start a new one;
		// already-published tiles remain last-known-good until replaced.
		checkpoint = nil
	}
	checkpoint, reset := resumeProgressiveCheckpoint(
		checkpoint, options.RunID, options.Tier, signature, len(publicationTiles), time.Now(),
	)
	if reset {
		if err := writeProgressiveCheckpoint(checkpointPath, checkpoint); err != nil {
			return nil, fmt.Errorf("initial checkpoint: %w", err)
		}
	}

	completed := len(checkpoint.Completed)
	progressUnitsPerTile := 1000
	for _, tilePlan := range publicationTiles {
		outputPath := filepath.Join(tileDir, tilePlan.name)
		if _, ok := checkpoint.Completed[tilePlan.name]; ok {
			if info, statErr := os.Stat(outputPath); statErr == nil && info.Size() > 0 {
				if progress != nil {
					progress(completed*progressUnitsPerTile, len(publicationTiles)*progressUnitsPerTile)
				}
				continue
			}
			delete(checkpoint.Completed, tilePlan.name)
			completed--
		}

		tileWidth, tileHeight := tilePlan.x1-tilePlan.x0, tilePlan.y1-tilePlan.y0
		computeWidth, computeHeight := tileWidth*supersample, tileHeight*supersample
		baseUnits := completed * progressUnitsPerTile
		margins, err := engine.MarginsChunked(
			tilePlan.bounds, zoom, cacheDir, tileURLBase, client, sites,
			computeWidth, computeHeight, propagation.LinkBudgetMaxRangeKm(p), p,
			supersample,
			func(done, total int) {
				if progress == nil || total <= 0 {
					return
				}
				localUnits := int(math.Min(1, float64(done)/float64(total)) * float64(progressUnitsPerTile))
				progress(baseUnits+localUnits, len(publicationTiles)*progressUnitsPerTile)
			},
		)
		if err != nil {
			return checkpointTiles(checkpoint), fmt.Errorf("progressive tile %s: %w", tilePlan.name, err)
		}
		img := marginsToImage(margins, tileWidth, tileHeight, p, maxAlpha)
		if err := WritePNG(outputPath, img); err != nil {
			return checkpointTiles(checkpoint), fmt.Errorf("publishing tile %s: %w", tilePlan.name, err)
		}
		checkpoint.Completed[tilePlan.name] = Tile{
			Image:  filepath.ToSlash(filepath.Join(append(publicParts, tilePlan.name)...)),
			Bounds: tilePlan.bounds,
		}
		completed++
		if err := writeProgressiveCheckpoint(checkpointPath, checkpoint); err != nil {
			return checkpointTiles(checkpoint), fmt.Errorf("checkpointing tile %s: %w", tilePlan.name, err)
		}
		if options.OnTile != nil {
			if err := options.OnTile(completed, len(publicationTiles), checkpointTiles(checkpoint)); err != nil {
				return checkpointTiles(checkpoint), err
			}
		}
		if progress != nil {
			progress(completed*progressUnitsPerTile, len(publicationTiles)*progressUnitsPerTile)
		}
	}
	checkpoint.Complete = true
	if err := writeProgressiveCheckpoint(checkpointPath, checkpoint); err != nil {
		return checkpointTiles(checkpoint), fmt.Errorf("completing checkpoint: %w", err)
	}
	return checkpointTiles(checkpoint), nil
}
