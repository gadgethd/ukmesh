// Per-repeater RF footprint requests.
//
// The browser map can show one repeater's own coverage ("Repeater RF
// footprint"). That raster is not part of the whole-region publication: it
// is computed by `hopreach -node <public key>`, which takes roughly half a
// minute on CPU and therefore cannot run inside the HTTP request that asked
// for it. This file accepts the request, bounds and deduplicates the queue,
// and runs jobs one at a time in the background — the same spawn pattern as
// admin.go's whole-region recompute trigger.
//
// The calculator's singleton lock (cmd/hopreach/lock.go) also serialises
// node jobs against the nightly whole-region run. A node job that arrives
// during that run reports "busy"; this queue retries it until the lock
// frees, so the nightly pass is never disturbed.
package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	nodeRequestPrefix = "/api/rf/node/"
	nodeQueueLimit    = 32
	nodeLogPrefix     = "shareapi: rf-node: "
	// nodeBusyRetryDelay is how long a queued job waits before retrying
	// after the calculator reported another run in progress. The initial
	// retries are quick (in case the other run is just finishing); after
	// that the delay grows so a long Precision pass doesn't fill the log.
	nodeBusyRetryDelay     = 30 * time.Second
	nodeBusyRetryDelaySlow = 5 * time.Minute
	nodeBusyFastAttempts   = 10
	// nodeBusyMaxWait bounds that retrying so a queue cannot sit forever
	// behind a failed or very long run. The nightly whole-region pass and
	// its Precision tier can run for hours on CPU, and both share the same
	// output lock; waiting six hours covers a full nightly pass while still
	// releasing a genuinely stuck request.
	nodeBusyMaxWait = 6 * time.Hour
)

type nodeJob struct {
	publicKey string
	queuedAt  time.Time
	attempts  int
}

var nodeJobs = struct {
	mu      sync.Mutex
	pending map[string]bool
	order   []nodeJob
	wake    chan struct{}
}{
	pending: make(map[string]bool),
	wake:    make(chan struct{}, 1),
}

// normalizeNodePublicKey mirrors cmd/hopreach's own -node validation. The
// key becomes an exec argument, so it must be exactly 64 hexadecimal
// characters and nothing else.
func normalizeNodePublicKey(value string) (string, bool) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 64 {
		return "", false
	}
	if _, err := hex.DecodeString(value); err != nil {
		return "", false
	}
	return value, true
}

// handleNodeRequest accepts one on-demand footprint request. It returns as
// soon as the job is queued: the result is published into meta.json, which
// the map already polls every few seconds.
func handleNodeRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}

	key, ok := normalizeNodePublicKey(strings.TrimPrefix(r.URL.Path, nodeRequestPrefix))
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "public key must be exactly 64 hexadecimal characters"})
		return
	}

	nodeJobs.mu.Lock()
	if nodeJobs.pending[key] {
		nodeJobs.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "coalesced", "public_key": key})
		return
	}
	if len(nodeJobs.order) >= nodeQueueLimit {
		nodeJobs.mu.Unlock()
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "rf footprint queue is full"})
		return
	}
	nodeJobs.pending[key] = true
	nodeJobs.order = append(nodeJobs.order, nodeJob{publicKey: key, queuedAt: time.Now()})
	nodeJobs.mu.Unlock()

	select {
	case nodeJobs.wake <- struct{}{}:
	default:
	}

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "queued", "public_key": key})
}

// runNodeQueue is a single runner: the calculator is CPU-bound and already
// has one singleton lock, so extra concurrency would only produce "busy"
// processes.
func runNodeQueue() {
	for {
		nodeJobs.mu.Lock()
		if len(nodeJobs.order) == 0 {
			nodeJobs.mu.Unlock()
			<-nodeJobs.wake
			continue
		}
		job := nodeJobs.order[0]
		nodeJobs.order = nodeJobs.order[1:]
		nodeJobs.mu.Unlock()

		if !runNodeJob(job) {
			// Another calculator run holds the lock: keep this job at the
			// front of the queue and try again after a pause.
			job.attempts++
			delay := nodeBusyRetryDelay
			if job.attempts > nodeBusyFastAttempts {
				delay = nodeBusyRetryDelaySlow
			}
			nodeJobs.mu.Lock()
			nodeJobs.order = append([]nodeJob{job}, nodeJobs.order...)
			nodeJobs.mu.Unlock()
			time.Sleep(delay)
			continue
		}

		nodeJobs.mu.Lock()
		delete(nodeJobs.pending, job.publicKey)
		nodeJobs.mu.Unlock()
	}
}

// runNodeJob returns true when the job is finished (completed or failed
// permanently) and false when it should be retried after a busy report.
func runNodeJob(job nodeJob) bool {
	cmd := exec.Command(hopreachBinary, "-node", job.publicKey)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	var result struct {
		State   string `json:"state"`
		Message string `json:"message"`
	}
	if line := lastJSONLine(stdout.String()); line != "" {
		_ = json.Unmarshal([]byte(line), &result)
	}

	if result.State == "busy" {
		if time.Since(job.queuedAt) < nodeBusyMaxWait {
			log.Printf("%s%s: calculator busy with another run, will retry", nodeLogPrefix, shortKey(job.publicKey))
			return false
		}
		log.Printf("%s%s: gave up after %s (calculator still busy)", nodeLogPrefix, shortKey(job.publicKey), nodeBusyMaxWait)
		return true
	}
	if runErr != nil {
		log.Printf("%s%s: failed: %v %s", nodeLogPrefix, shortKey(job.publicKey), runErr, strings.TrimSpace(stderr.String()))
		return true
	}
	log.Printf("%s%s: %s %s", nodeLogPrefix, shortKey(job.publicKey), result.State, result.Message)
	return true
}

// lastJSONLine finds the calculator's single JSON result line. Progress
// logging from the coverage pipeline also goes to stdout, so the last line
// that starts with "{" is the result.
func lastJSONLine(output string) string {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, "{") {
			return line
		}
	}
	return ""
}

func shortKey(key string) string {
	if len(key) <= 12 {
		return key
	}
	return key[:12]
}
