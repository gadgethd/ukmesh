package compute

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"hopreach/internal/demgrid"
	"hopreach/internal/gpujob"
	"hopreach/internal/propagation"
)

// ErrRemoteParked indicates a retry can continue using the same checkpoint.
var ErrRemoteParked = errors.New("waiting for the remote GPU worker")

const (
	remoteProbeTimeout = 3 * time.Second
	remoteJobTimeout   = 30*time.Minute + 5*time.Second
	remoteJSONLimit    = 16 * 1024
)

// RequireWorker checks remote-only mode before fetching terrain or replacing
// metadata. Other modes retain their existing backend selection policy.
func (e *Engine) RequireWorker() error {
	if e.remoteOnly && !e.remoteAvailable() {
		return ErrRemoteParked
	}
	return nil
}

func (e *Engine) dispatchRemote(grid *demgrid.Grid, sites []propagation.Site, bounds propagation.Bounds, width, height int, rangeKm float64, p propagation.Params, progressFn func(done, total int)) ([]float32, error) {
	if !e.remoteConfigured() {
		return nil, ErrRemoteParked
	}
	e.reportBackend("remote_gpu")
	margins, err := e.marginsRemote(grid, sites, bounds, width, height, rangeKm, p, progressFn)
	if err != nil && e.remoteOnly {
		return nil, fmt.Errorf("%w: %w", ErrRemoteParked, err)
	}
	return margins, err
}

func (e *Engine) brokerRequest(ctx context.Context, method, route string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+e.brokerAddr+route, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	return http.DefaultClient.Do(req)
}

// remoteConfigured reports whether a broker address was configured at all —
// if not, Margins skips the remote branch entirely rather than making a
// doomed HTTP call to nothing on every single pass.
func (e *Engine) remoteConfigured() bool {
	return e.brokerAddr != ""
}

// remoteAvailable additionally confirms a worker is actually connected right
// now — used by Available (and hence the per-tier GPU-gating check), which
// needs to know before committing to a whole pass, not just after a submit
// attempt already failed.
func (e *Engine) remoteAvailable() bool {
	connected, _ := e.remoteStatus()
	return connected
}

// remoteStatus queries the broker's /gpu/status once, returning whether a
// worker is connected and (if so) its self-reported available memory (see
// gpujob.Hello) — split out from remoteAvailable so MarginsChunked's
// chunk-budget auto-sizing can get both in one round trip instead of two.
// AvailableBytes is 0 if no worker is connected or the connected one
// predates Hello (unknown, not "zero RAM").
func (e *Engine) remoteStatus() (connected bool, availableBytes uint64) {
	if !e.remoteConfigured() {
		return false, 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), remoteProbeTimeout)
	defer cancel()
	resp, err := e.brokerRequest(ctx, http.MethodGet, "/gpu/status", nil)
	if err != nil {
		return false, 0
	}
	defer resp.Body.Close()
	var status struct {
		WorkerConnected bool   `json:"worker_connected"`
		AvailableBytes  uint64 `json:"available_bytes"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, remoteJSONLimit)).Decode(&status) != nil {
		return false, 0
	}
	return status.WorkerConnected, status.AvailableBytes
}

func (e *Engine) nextJobID() string {
	e.jobMu.Lock()
	defer e.jobMu.Unlock()
	e.jobSeq++
	return fmt.Sprintf("job-%d-%d", time.Now().UnixNano(), e.jobSeq)
}

// remoteProgressPollInterval is how often marginsRemote polls
// /gpu/progress while its one /gpu/submit call is still outstanding —
// frequent enough to feel live in the UI, infrequent enough that polling
// itself is never a meaningful cost next to an actual coverage pass.
const remoteProgressPollInterval = 500 * time.Millisecond

// marginsRemote submits one whole coverage pass to the broker and blocks
// until the result arrives (or the broker's own job timeout fires and it
// responds with an error). The elevation grid itself is never sent — only
// bounds, sites, and propagation parameters; the worker fetches/caches its
// own DEM tiles from e.demTileURLBase, since a low-powered VPS likely also
// means a modest-bandwidth link not worth spending on shipping a multi-GB
// grid.
//
// The actual submit call still only completes once, in one HTTP round
// trip — but while it's outstanding, this polls the broker's separately
// tracked per-job progress (fed by Progress frames the worker sends over
// its WebSocket connection as gpucompute.ComputeMargins' own dispatch loop
// reports rows done) so progressFn sees real incremental updates instead
// of just an opening 0/total and a closing total/total either side of one
// long silent wait.
func (e *Engine) marginsRemote(grid *demgrid.Grid, sites []propagation.Site, bounds propagation.Bounds, imageWidth, imageHeight int, rangeKm float64, p propagation.Params, progressFn func(done, total int)) ([]float32, error) {
	job := gpujob.Job{
		ID:             e.nextJobID(),
		Sites:          sites,
		Bounds:         bounds,
		DemBounds:      padBounds(bounds, rangeKm),
		ImageWidth:     imageWidth,
		ImageHeight:    imageHeight,
		RangeKm:        rangeKm,
		Propagation:    p,
		DemZoom:        grid.Zoom,
		DemTileURLBase: e.demTileURLBase,
	}
	body, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("remote GPU: encoding job: %w", err)
	}

	if progressFn != nil {
		progressFn(0, imageHeight)
	}

	type submitOutcome struct {
		data []byte
		err  error
	}
	outcomeCh := make(chan submitOutcome, 1)
	timeout := e.remoteTimeout
	if timeout <= 0 {
		timeout = remoteJobTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	go func() {
		data, err := e.submitRemoteJob(ctx, body, imageWidth, imageHeight)
		outcomeCh <- submitOutcome{data: data, err: err}
	}()

	ticker := time.NewTicker(remoteProgressPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case outcome := <-outcomeCh:
			if outcome.err != nil {
				return nil, outcome.err
			}
			margins := gpujob.BytesToFloat32LE(outcome.data)
			if progressFn != nil {
				progressFn(imageHeight, imageHeight)
			}
			return margins, nil
		case <-ticker.C:
			if progressFn == nil {
				continue
			}
			if done, total := e.pollRemoteProgress(ctx, job.ID); total > 0 {
				progressFn(done, total)
			}
		}
	}
}

// submitRemoteJob does the actual blocking POST /gpu/submit call —
// factored out of marginsRemote so that function can run it in a goroutine
// and poll progress concurrently while it's outstanding.
func (e *Engine) submitRemoteJob(ctx context.Context, body []byte, imageWidth, imageHeight int) ([]byte, error) {
	resp, err := e.brokerRequest(ctx, http.MethodPost, "/gpu/submit", body)
	if err != nil {
		return nil, fmt.Errorf("remote GPU: submitting job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var errResp gpujob.Result
		_ = json.NewDecoder(io.LimitReader(resp.Body, remoteJSONLimit)).Decode(&errResp)
		if errResp.Error != "" {
			return nil, fmt.Errorf("remote GPU: %s", errResp.Error)
		}
		return nil, fmt.Errorf("remote GPU: broker returned status %d", resp.StatusCode)
	}

	expected := imageWidth * imageHeight * 4
	data, err := io.ReadAll(io.LimitReader(resp.Body, int64(expected)+1))
	if err != nil {
		return nil, fmt.Errorf("remote GPU: reading result: %w", err)
	}
	if len(data) != expected {
		return nil, fmt.Errorf("remote GPU: result size %d doesn't match expected %d", len(data), expected)
	}
	return data, nil
}

// pollRemoteProgress is a best-effort read of the broker's tracked
// progress for jobID — any failure (network hiccup, broker briefly
// unreachable) just means this poll contributes no update, never an error
// that could disrupt the real submit/result flow above.
func (e *Engine) pollRemoteProgress(jobContext context.Context, jobID string) (done, total int) {
	ctx, cancel := context.WithTimeout(jobContext, remoteProbeTimeout)
	defer cancel()
	resp, err := e.brokerRequest(ctx, http.MethodGet, "/gpu/progress?id="+jobID, nil)
	if err != nil {
		return 0, 0
	}
	defer resp.Body.Close()
	var p struct{ Done, Total int }
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, remoteJSONLimit)).Decode(&p) != nil {
		return 0, 0
	}
	return p.Done, p.Total
}
