// Remote GPU broker: relays coverage-compute jobs between the batch job
// (/app/hopreach, same container, calling POST /gpu/submit over
// localhost) and a remote GPU worker (cmd/hopreach-gpuworker, a separate container
// on a different machine, connected over WebSocket at GET /gpu-worker,
// proxied by nginx since it's the one part of this that needs to be
// reachable from outside).
//
// Deliberately simple: exactly one worker connection at a time (a new one
// replaces whatever was there, logged) and exactly one job in flight at a
// time (the batch job submits passes sequentially, never concurrently) —
// matches the actual usage pattern rather than building out a queue this
// project doesn't need yet.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"hopreach/internal/gpujob"
)

var gpuUpgrader = websocket.Upgrader{
	// No origin check: this isn't a browser client, it's a purpose-built
	// worker binary presenting a bearer token — CheckOrigin's browser-CSRF
	// threat model doesn't apply here.
	CheckOrigin: func(r *http.Request) bool { return true },
}

type gpuJobResult struct {
	margins []byte
	err     string
}

type pendingGPUJob struct {
	id          string
	resultBytes int
	result      chan gpuJobResult
	done, total int
	hasProgress bool
}

type gpuBroker struct {
	mu      sync.Mutex
	conn    *websocket.Conn
	writeMu sync.Mutex // serializes writes to conn without blocking status reads
	pending *pendingGPUJob
	hello   gpujob.Hello // zero value = unknown
}

var broker = &gpuBroker{}

// workerReadTimeout is how long the broker will wait for ANY frame from a
// connected worker before treating the connection as dead. Comfortably
// more than the worker's own 2-minute Hello heartbeat (see
// cmd/hopreach-gpuworker's helloInterval) so an idle-but-healthy worker is
// never dropped, while still noticing a genuinely dead socket in minutes
// rather than however long the OS takes to give up on a half-open TCP
// connection. The worker reconnects on its own, so a false positive here
// costs one reconnect, not a lost worker.
const workerReadTimeout = 6 * time.Minute
const workerControlLimit = 16 << 10

// Default is generous (30 min), not a few seconds' safety margin: a large
// Precision-tier job on a worker with a cold DEM tile cache can spend
// several minutes just fetching tiles from the upstream source before GPU
// compute even starts (observed in practice: ~7 minutes for a whole-
// Scotland zoom-13 grid on a fresh cache) — a short timeout here would
// silently discard an otherwise-successful remote result and fall back to
// CPU for no good reason, defeating the point of having a worker at all.
func gpuJobTimeout() time.Duration {
	f := cfg.RemoteWorker.JobTimeoutSeconds
	if f <= 0 {
		f = 1800
	}
	return time.Duration(f * float64(time.Second))
}

func (b *gpuBroker) connected() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.conn != nil
}

// clearConnIf drops the registered connection only if it's still c, and
// reports whether it did.
//
// This has to be compare-and-clear, not a plain clear: a read loop finds
// out its socket is dead whenever the OS finally gives up, which for a
// half-open TCP connection can be long after the worker noticed, gave up
// and reconnected. The reconnect registers the new connection; then the
// old loop wakes up and tears down — and an unconditional clear wipes the
// live registration on its way out. The broker then reports no worker
// connected for as long as that new connection lasts, while the worker
// sits idle believing it's connected, and every coverage job silently
// falls back to CPU. Seen in production: a worker reconnected at 03:25 and
// was still being reported disconnected five hours later, with a whole
// Precision tier running on CPU at roughly 13x the wall-clock cost.
func (b *gpuBroker) clearConnIf(c *websocket.Conn, reason string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != c {
		return false // a newer connection already took over — leave it alone
	}
	b.clearConnLocked(reason)
	return true
}

func (b *gpuBroker) clearConnLocked(reason string) {
	b.conn = nil
	b.hello = gpujob.Hello{}
	b.finishPendingLocked(gpuJobResult{err: reason})
}

func (b *gpuBroker) setConn(c *websocket.Conn) (old *websocket.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	old = b.conn
	if old != nil && old != c {
		b.finishPendingLocked(gpuJobResult{err: "worker connection replaced"})
	}
	b.conn = c
	// A new (or no) connection means whatever was previously reported is no
	// longer trustworthy — a replacement worker could be a different box
	// entirely, and no connection at all means no worker to size tiles
	// against. The new connection's own Hello (if any) will set this again.
	b.hello = gpujob.Hello{}
	return old
}

// setHello records the worker's self-reported memory/hardware info (see
// gpujob.Hello), sent once on connect and then repeated periodically as a
// heartbeat — read by handleGPUStatus (and from there by compute.Engine's
// chunk-budget auto-sizing) and by the analytics endpoint's hardware panel
// and memory-sample collection.
func (b *gpuBroker) setHello(c *websocket.Conn, h gpujob.Hello) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != c {
		return
	}
	b.hello = h
	recordGPUWorkerHardware(h)
}

func (b *gpuBroker) getHello() gpujob.Hello {
	h, _ := b.workerStatus()
	return h
}

func (b *gpuBroker) workerStatus() (gpujob.Hello, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hello, b.conn != nil
}

func (b *gpuBroker) finishPendingLocked(result gpuJobResult) {
	if b.pending != nil {
		b.pending.result <- result
		b.pending = nil
	}
}

func (b *gpuBroker) pendingFor(c *websocket.Conn, id string) *pendingGPUJob {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn == c && b.pending != nil && b.pending.id == id {
		return b.pending
	}
	return nil
}

func (b *gpuBroker) deliver(c *websocket.Conn, pending *pendingGPUJob, result gpuJobResult) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn == c && b.pending == pending {
		b.finishPendingLocked(result)
	}
}

// setProgress records the latest (done, total) reported for an in-flight
// job — see handleGPUProgress, polled by internal/compute's remote-dispatch
// path while it waits on /gpu/submit.
func (b *gpuBroker) setProgress(c *websocket.Conn, p gpujob.Progress) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn == c && b.pending != nil && b.pending.id == p.ID {
		b.pending.done = p.Done
		b.pending.total = p.Total
		b.pending.hasProgress = true
	}
}

func (b *gpuBroker) getProgress(id string) (done, total int, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pending == nil || b.pending.id != id {
		return 0, 0, false
	}
	return b.pending.done, b.pending.total, b.pending.hasProgress
}

// submit sends job to the connected worker and blocks until a result
// arrives or timeout elapses. Returns an error (never blocks forever) if
// no worker is connected, the send fails, or nothing comes back in time.
func (b *gpuBroker) submit(job gpujob.Job, timeout time.Duration) ([]byte, error) {
	const maxInt = int(^uint(0) >> 1)
	if job.ID == "" || job.ImageWidth <= 0 || job.ImageHeight <= 0 || job.ImageWidth > maxInt/4/job.ImageHeight {
		return nil, fmt.Errorf("invalid GPU job ID or raster dimensions")
	}
	body, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("encoding job: %w", err)
	}

	b.mu.Lock()
	conn := b.conn
	if conn == nil {
		b.mu.Unlock()
		return nil, fmt.Errorf("no GPU worker connected")
	}
	if b.pending != nil {
		b.mu.Unlock()
		return nil, fmt.Errorf("remote GPU worker is busy")
	}
	pending := &pendingGPUJob{
		id:          job.ID,
		resultBytes: 4 * job.ImageWidth * job.ImageHeight,
		result:      make(chan gpuJobResult, 1),
	}
	b.pending = pending
	b.mu.Unlock()

	b.writeMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	writeErr := conn.WriteMessage(websocket.TextMessage, body)
	_ = conn.SetWriteDeadline(time.Time{})
	b.writeMu.Unlock()
	if writeErr != nil {
		b.clearConnIf(conn, "worker write failed")
		_ = conn.Close()
		return nil, fmt.Errorf("sending job to worker: %w", writeErr)
	}

	var result gpuJobResult
	select {
	case result = <-pending.result:
	case <-time.After(timeout):
		// The worker has no cancellation message. End this connection rather
		// than queue new work behind a timed out job on the same socket.
		b.mu.Lock()
		if b.conn == conn && b.pending == pending {
			b.clearConnLocked("worker job timed out")
			b.mu.Unlock()
			_ = conn.Close()
			return nil, fmt.Errorf("timed out after %s waiting for worker", timeout)
		}
		b.mu.Unlock()
		result = <-pending.result // a result or connection teardown won the race
	}
	if result.err != "" {
		return nil, fmt.Errorf("worker reported: %s", result.err)
	}
	return result.margins, nil
}

func (b *gpuBroker) readLoop(conn *websocket.Conn) {
	defer conn.Close()
	err := b.readWorkerMessages(conn)
	if b.clearConnIf(conn, fmt.Sprintf("worker disconnected: %v", err)) {
		log.Printf("gpubroker: worker disconnected: %v", err)
	}
}

func (b *gpuBroker) readWorkerMessages(conn *websocket.Conn) error {
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(workerReadTimeout))
	})
	var awaiting *pendingGPUJob
	var sawInterleavedHello bool
	for {
		limit := workerControlLimit
		if awaiting != nil {
			limit = max(limit, awaiting.resultBytes)
		}
		conn.SetReadLimit(int64(limit))
		if err := conn.SetReadDeadline(time.Now().Add(workerReadTimeout)); err != nil {
			return err
		}
		msgType, message, err := conn.NextReader()
		if err != nil {
			return err
		}
		if awaiting != nil && msgType == websocket.BinaryMessage {
			if b.pendingFor(conn, awaiting.id) != awaiting {
				return fmt.Errorf("no pending job for worker result")
			}
			margins, err := readMargins(message, awaiting.resultBytes)
			if err != nil {
				return err
			}
			b.deliver(conn, awaiting, gpuJobResult{margins: margins})
			awaiting = nil
			continue
		}
		if msgType != websocket.TextMessage {
			return fmt.Errorf("unexpected binary frame from GPU worker")
		}

		data, err := io.ReadAll(io.LimitReader(message, workerControlLimit+1))
		if err != nil {
			return err
		}
		if len(data) > workerControlLimit {
			return fmt.Errorf("GPU worker control message exceeds %d bytes", workerControlLimit)
		}
		var head struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(data, &head); err != nil {
			return fmt.Errorf("malformed GPU worker control message: %w", err)
		}
		if awaiting != nil {
			// Existing workers serialize their heartbeat writes with each
			// result message separately, so one Hello can arrive between them.
			if head.Kind != gpujob.KindHello || sawInterleavedHello {
				return fmt.Errorf("expected binary GPU margins after result")
			}
			sawInterleavedHello = true
		}

		switch head.Kind {
		case gpujob.KindHello:
			var h gpujob.Hello
			if err := json.Unmarshal(data, &h); err != nil {
				return fmt.Errorf("malformed GPU worker hello: %w", err)
			}
			b.setHello(conn, h)
		case gpujob.KindProgress:
			var p gpujob.Progress
			if err := json.Unmarshal(data, &p); err != nil {
				return fmt.Errorf("malformed GPU worker progress: %w", err)
			}
			b.setProgress(conn, p)
		case gpujob.KindResult, "":
			var result gpujob.Result
			if err := json.Unmarshal(data, &result); err != nil {
				return fmt.Errorf("malformed GPU worker result: %w", err)
			}
			pending := b.pendingFor(conn, result.ID)
			if pending == nil {
				return fmt.Errorf("no pending job for worker result")
			}
			if result.Error != "" {
				b.deliver(conn, pending, gpuJobResult{err: result.Error})
				continue
			}
			awaiting, sawInterleavedHello = pending, false
		default:
			return fmt.Errorf("unknown GPU worker message kind")
		}
	}
}

func readMargins(message io.Reader, expected int) ([]byte, error) {
	margins := make([]byte, expected)
	if _, err := io.ReadFull(message, margins); err != nil {
		return nil, fmt.Errorf("reading %d GPU margin bytes: %w", expected, err)
	}
	var extra [1]byte
	_, err := io.ReadFull(message, extra[:])
	if err == nil {
		return nil, fmt.Errorf("GPU margin result exceeds %d bytes", expected)
	}
	if err != io.EOF {
		return nil, fmt.Errorf("checking end of GPU margin result: %w", err)
	}
	return margins, nil
}

// handleGPUWorkerConnect upgrades a WebSocket connection from a remote GPU
// worker. Requires GPU_WORKER_TOKEN to match — this endpoint is reachable
// from the public internet (nginx proxies it), so it's a real trust
// boundary: whoever holds the token can feed data into the live public
// coverage map. Never registered at all if the token isn't configured (see
// main.go) rather than defaulting to an open endpoint.
func handleGPUWorkerConnect(requiredToken string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Authorization")
		if got != "Bearer "+requiredToken {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		conn, err := gpuUpgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("gpubroker: websocket upgrade failed: %v", err)
			return
		}
		if old := broker.setConn(conn); old != nil {
			log.Printf("gpubroker: new worker connection replacing a previous one")
			old.Close()
		} else {
			log.Printf("gpubroker: GPU worker connected")
		}
		go broker.readLoop(conn)
	}
}

// handleGPUSubmit is local-only in practice (never proxied by nginx — only
// /app/hopreach, in the same container, ever calls it) — takes one whole
// coverage pass's job description and blocks until the worker's result
// arrives, returning the margins as raw octet-stream bytes.
func handleGPUSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var job gpujob.Job
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "invalid job JSON", http.StatusBadRequest)
		return
	}
	margins, err := broker.submit(job, gpuJobTimeout())
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		if err.Error() == "no GPU worker connected" {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(gpujob.Result{ID: job.ID, Error: err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(margins)
}

// handleGPUProgress reports the latest (done, total) reported for an
// in-flight job — local-only in practice, same as handleGPUSubmit, polled
// by internal/compute's remote-dispatch path while its one blocking
// /gpu/submit call for the same job ID is still outstanding. Not an error
// if nothing's tracked yet (a job that hasn't reported any progress frames
// yet, or one that already finished) — just reports zeros, same as the
// "haven't started" state every other compute path already reports before
// its first real sample.
func handleGPUProgress(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	done, total, _ := broker.getProgress(id)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"done": done, "total": total})
}

// handleGPUStatus reports whether a worker is currently connected — used
// both by the remote-dispatch path (skip the doomed /gpu/submit call
// entirely if nothing's connected) and the per-tier GPU-gating check in
// main.go (decide whether to attempt a gated tier at all). The remaining
// fields are the connected worker's self-reported memory/hardware info (see
// gpujob.Hello), zero-valued if unknown (no worker connected, or a worker
// that predates Hello) — available_bytes specifically is read by
// compute.Engine's chunk-budget auto-sizing, the rest by the analytics
// endpoint's hardware panel.
func handleGPUStatus(w http.ResponseWriter, r *http.Request) {
	h, connected := broker.workerStatus()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"worker_connected": connected,
		"available_bytes":  h.AvailableBytes,
		"total_bytes":      h.TotalBytes,
		"cpu_model":        h.CPUModel,
		"gpu_adapter":      h.GPUAdapter,
	})
}
