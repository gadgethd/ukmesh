package main

import (
	"bytes"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestScheduledLockWaitsThenAcquires(t *testing.T) {
	path := filepath.Join(t.TempDir(), "calculator.lock")
	held, err := acquireLockAt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if other, err := acquireLockAt(path); !errors.Is(err, errLockBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("ordinary lock acquisition should return busy immediately: %v", err)
	}
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	attempts := 0
	lock, err := waitForLock(func() (*os.File, error) {
		attempts++
		if attempts == 3 {
			held.Close()
		}
		return acquireLockAt(path)
	}, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if attempts != 3 || strings.Count(output.String(), "scheduled run waiting for calculator lock") != 2 {
		t.Fatalf("attempts=%d, log=%s", attempts, &output)
	}
}

func TestScheduledLockTimeoutAndPermanentError(t *testing.T) {
	_, err := waitForLock(func() (*os.File, error) { return nil, errLockBusy }, 3*time.Millisecond, time.Millisecond)
	if !errors.Is(err, errLockBusy) || !strings.Contains(err.Error(), "timed out after 3ms waiting for calculator lock") {
		t.Fatalf("expected explicit lock timeout, got %v", err)
	}
	permanent := errors.New("lock file permission denied")
	attempts := 0
	_, err = waitForLock(func() (*os.File, error) { attempts++; return nil, permanent }, time.Hour, 15*time.Second)
	if !errors.Is(err, permanent) || attempts != 1 {
		t.Fatalf("permanent lock error should fail immediately: %v (%d attempts)", err, attempts)
	}
}

func TestScheduledLockDoesNotAcquireAfterDeadline(t *testing.T) {
	attempts := 0
	lock, err := waitForLock(func() (*os.File, error) {
		attempts++
		if attempts == 1 {
			return nil, errLockBusy
		}
		return os.Open(os.DevNull)
	}, time.Millisecond, time.Hour)
	if lock != nil {
		lock.Close()
	}
	if !errors.Is(err, errLockBusy) || attempts != 1 {
		t.Fatalf("expired wait must not acquire even if the lock has become free: %v (%d attempts)", err, attempts)
	}
}
