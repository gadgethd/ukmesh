package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"hopreach/internal/gpujob"
)

func TestBrokerAcceptsOnlyOneInFlightJob(t *testing.T) {
	existing := &pendingGPUJob{id: "existing"}
	b := &gpuBroker{conn: &websocket.Conn{}, pending: existing}
	job := gpujob.Job{ID: "second", ImageWidth: 1, ImageHeight: 1}
	if _, err := b.submit(job, time.Second); err == nil || !strings.Contains(err.Error(), "busy") {
		t.Fatalf("a second submission must report busy: %v", err)
	}
	if b.pending != existing {
		t.Fatal("the second submission interfered with the existing job")
	}
}

func TestBrokerProgressTracking(t *testing.T) {
	conn := &websocket.Conn{}
	b := &gpuBroker{conn: conn}
	b.setProgress(conn, gpujob.Progress{ID: "unknown", Done: 5, Total: 10})
	if _, _, ok := b.getProgress("unknown"); ok {
		t.Error("tracked progress for an unknown job")
	}

	first := &pendingGPUJob{id: "job-1", result: make(chan gpuJobResult, 1)}
	b.pending = first
	b.setProgress(conn, gpujob.Progress{ID: first.id, Done: 3, Total: 10})
	if done, total, ok := b.getProgress(first.id); !ok || done != 3 || total != 10 {
		t.Fatalf("getProgress = (%d, %d, %v), want (3, 10, true)", done, total, ok)
	}
	b.setProgress(conn, gpujob.Progress{ID: first.id, Done: 7, Total: 10})
	if done, _, _ := b.getProgress(first.id); done != 7 {
		t.Errorf("expected progress to update to 7, got %d", done)
	}

	b.deliver(conn, first, gpuJobResult{margins: []byte{1, 2, 3, 4}})
	if _, _, ok := b.getProgress(first.id); ok {
		t.Error("expected progress to be cleared after delivery")
	}

	second := &pendingGPUJob{id: "job-2", result: make(chan gpuJobResult, 1)}
	b.pending = second
	b.setProgress(conn, gpujob.Progress{ID: second.id, Done: 1, Total: 5})
	b.clearConnIf(conn, "worker disconnected")
	if _, _, ok := b.getProgress(second.id); ok {
		t.Error("expected progress to be cleared after disconnect")
	}
}

func TestReplacedWorkerCannotChangeLiveState(t *testing.T) {
	first, second := &websocket.Conn{}, &websocket.Conn{}
	b := &gpuBroker{conn: first}
	b.setHello(first, gpujob.Hello{AvailableBytes: 4_700_000_000})
	if got := b.getHello().AvailableBytes; got != 4_700_000_000 {
		t.Fatalf("first Hello available bytes = %d", got)
	}
	oldJob := &pendingGPUJob{id: "reused-id", resultBytes: 4, result: make(chan gpuJobResult, 1)}
	b.pending = oldJob
	if old := b.setConn(second); old != first {
		t.Fatal("replacement did not return the first connection")
	}
	if got := <-oldJob.result; got.err != "worker connection replaced" {
		t.Fatalf("replaced job result = %+v", got)
	}
	if got := b.getHello().AvailableBytes; got != 0 {
		t.Fatalf("inherited the old Hello: %d", got)
	}

	newJob := &pendingGPUJob{id: oldJob.id, resultBytes: 8, result: make(chan gpuJobResult, 1)}
	b.pending = newJob
	b.setHello(second, gpujob.Hello{AvailableBytes: 2_000})
	b.setProgress(second, gpujob.Progress{ID: newJob.id, Done: 1, Total: 2})

	b.setHello(first, gpujob.Hello{AvailableBytes: 1_000})
	b.setProgress(first, gpujob.Progress{ID: newJob.id, Done: 2, Total: 2})
	b.deliver(first, newJob, gpuJobResult{err: "late reply"})
	b.deliver(second, oldJob, gpuJobResult{err: "late job"})
	if b.pendingFor(first, newJob.id) != nil || b.pendingFor(second, newJob.id) != newJob {
		t.Fatal("result dimensions were exposed to the wrong worker")
	}
	if b.clearConnIf(first, "obsolete worker disconnected") || !b.connected() {
		t.Fatal("stale connection removed the active worker")
	}

	if got := b.getHello().AvailableBytes; got != 2_000 {
		t.Errorf("stale Hello replaced live memory with %d", got)
	}
	if done, total, ok := b.getProgress(newJob.id); done != 1 || total != 2 || !ok {
		t.Errorf("stale Progress changed live job to (%d, %d, %v)", done, total, ok)
	}
	select {
	case got := <-newJob.result:
		t.Fatalf("live job received a stale result: %+v", got)
	default:
	}
	if !b.clearConnIf(second, "live worker disconnected") || b.connected() {
		t.Fatal("live connection failed to unregister itself")
	}
}

func TestBrokerStaysConnectedWhenAStaleSocketClosesAfterAReconnect(t *testing.T) {
	b, dial := mockWorkerBroker(t)
	first := dial()
	waitForGPUWorker(t, b, true)
	second := dial()
	if err := second.WriteJSON(gpujob.Hello{Kind: gpujob.KindHello, AvailableBytes: 123}); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	deadline := time.Now().Add(time.Second)
	for b.getHello().AvailableBytes != 123 {
		if time.Now().After(deadline) {
			t.Fatal("replacement worker heartbeat was lost")
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = second.Close()
	waitForGPUWorker(t, b, false)
}

func TestBrokerWorkerReplies(t *testing.T) {
	const jobID = "mock-raster"
	result := gpujob.Result{Kind: gpujob.KindResult, ID: jobID}
	margins := gpujob.Float32ToBytesLE([]float32{-10.5, 3.5})
	send := func(t *testing.T, worker *websocket.Conn, kind int, payload []byte) {
		t.Helper()
		if err := worker.WriteMessage(kind, payload); err != nil {
			t.Fatal(err)
		}
	}
	sendJSON := func(t *testing.T, worker *websocket.Conn, v any) {
		t.Helper()
		if err := worker.WriteJSON(v); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name       string
		mock       func(*testing.T, *websocket.Conn)
		wantError  string
		disconnect bool
	}{
		{name: "exact binary payload with interleaved heartbeat", mock: func(t *testing.T, worker *websocket.Conn) {
			sendJSON(t, worker, result)
			sendJSON(t, worker, gpujob.Hello{Kind: gpujob.KindHello, AvailableBytes: 123})
			send(t, worker, websocket.BinaryMessage, margins)
		}},
		{name: "worker error with no binary frame", mock: func(t *testing.T, worker *websocket.Conn) {
			sendJSON(t, worker, gpujob.Result{ID: jobID, Error: "DEM unavailable"})
		}, wantError: "DEM unavailable"},
		{name: "control size limit", mock: func(t *testing.T, worker *websocket.Conn) {
			send(t, worker, websocket.TextMessage, bytes.Repeat([]byte{' '}, workerControlLimit+1))
		}, wantError: "read limit", disconnect: true},
		{name: "unexpected binary", mock: func(t *testing.T, worker *websocket.Conn) {
			send(t, worker, websocket.BinaryMessage, margins)
		}, wantError: "unexpected binary", disconnect: true},
		{name: "short margins", mock: func(t *testing.T, worker *websocket.Conn) {
			sendJSON(t, worker, result)
			send(t, worker, websocket.BinaryMessage, margins[:4])
		}, wantError: "reading 8 GPU margin bytes", disconnect: true},
		{name: "long margins", mock: func(t *testing.T, worker *websocket.Conn) {
			sendJSON(t, worker, result)
			send(t, worker, websocket.BinaryMessage, append(bytes.Clone(margins), 0))
		}, wantError: "exceeds 8 bytes", disconnect: true},
		{name: "unexpected control while reading margins", mock: func(t *testing.T, worker *websocket.Conn) {
			sendJSON(t, worker, result)
			sendJSON(t, worker, gpujob.Progress{Kind: gpujob.KindProgress, ID: jobID})
		}, wantError: "expected binary GPU margins", disconnect: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, dial := mockWorkerBroker(t)
			worker := dial()
			waitForGPUWorker(t, b, true)
			type submission struct {
				margins []byte
				err     error
			}
			replies := make(chan submission, 1)
			go func() {
				got, err := b.submit(gpujob.Job{ID: jobID, ImageWidth: 2, ImageHeight: 1}, 2*time.Second)
				replies <- submission{got, err}
			}()
			if err := worker.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			kind, data, err := worker.ReadMessage()
			var job gpujob.Job
			if err != nil || kind != websocket.TextMessage || json.Unmarshal(data, &job) != nil || job.ID != jobID {
				t.Fatalf("mock did not receive the submitted job: %s, %v", data, err)
			}
			tc.mock(t, worker)
			select {
			case reply := <-replies:
				if tc.wantError == "" {
					if reply.err != nil || !bytes.Equal(reply.margins, margins) {
						t.Fatalf("valid result rejected: %x, %v", reply.margins, reply.err)
					}
				} else if reply.err == nil || !strings.Contains(reply.err.Error(), tc.wantError) || reply.margins != nil {
					t.Fatalf("got margins=%x err=%v, want %q", reply.margins, reply.err, tc.wantError)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("broker did not finish the submission")
			}
			if b.connected() == tc.disconnect {
				t.Fatalf("worker connection does not match expected disconnect: %v", tc.disconnect)
			}
			_ = worker.Close()
			waitForGPUWorker(t, b, false)
		})
	}
}

func mockWorkerBroker(t *testing.T) (*gpuBroker, func() *websocket.Conn) {
	t.Helper()
	const token = "test-token"
	saved := broker
	b := &gpuBroker{}
	broker = b
	t.Cleanup(func() { broker = saved })
	srv := httptest.NewServer(handleGPUWorkerConnect(token))
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	return b, func() *websocket.Conn {
		c, _, err := websocket.DefaultDialer.Dial(url, http.Header{"Authorization": {"Bearer " + token}})
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
}

func waitForGPUWorker(t *testing.T, b *gpuBroker, want bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for b.connected() != want {
		if time.Now().After(deadline) {
			t.Fatalf("worker connected = %v, want %v", b.connected(), want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
