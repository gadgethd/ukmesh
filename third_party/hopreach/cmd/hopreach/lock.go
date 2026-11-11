package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"
)

// lockPath is process-wide (not per-config), deliberately: the fetch/compute
// pipeline can now be triggered from three independent places — the Docker
// entrypoint's own background initial run, the daily cron job, and (see
// cmd/hopreach-shareapi's /admin/recompute) an on-demand trigger — and
// letting two of them race, both writing repeaters.geojson/meta.json/the
// coverage tiles at once, would silently corrupt the output rather than
// fail loudly. A single well-known lock file makes that impossible
// regardless of which combination fires.
const lockPath = "/tmp/hopreach.lock"

const (
	scheduledLockTimeout      = time.Hour
	scheduledLockPollInterval = 15 * time.Second
)

var errLockBusy = errors.New("another run already in progress")

// acquireLock takes a non-blocking exclusive flock, releasing automatically
// if this process dies for any reason (flock is tied to the open file
// descriptor, not explicitly released) — no stale-lock cleanup needed.
// Returns an error immediately (not after waiting) if another run already
// holds it.
func acquireLock() (*os.File, error) {
	return acquireLockAt(lockPath)
}

func acquireLockAt(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errLockBusy
		}
		return nil, fmt.Errorf("locking calculator: %w", err)
	}
	return f, nil
}

func waitForLock(acquire func() (*os.File, error), timeout, pollInterval time.Duration) (*os.File, error) {
	deadline := time.Now().Add(timeout)
	for {
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("timed out after %s waiting for calculator lock: %w", timeout, errLockBusy)
		}
		lock, err := acquire()
		if !errors.Is(err, errLockBusy) {
			return lock, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, fmt.Errorf("timed out after %s waiting for calculator lock: %w", timeout, err)
		}
		wait := min(pollInterval, remaining)
		log.Printf("hopreach: scheduled run waiting for calculator lock; retry in %s (timeout in %s)", wait, remaining.Round(time.Second))
		time.Sleep(wait)
	}
}
