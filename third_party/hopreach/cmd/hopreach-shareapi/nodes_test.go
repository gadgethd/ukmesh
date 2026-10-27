package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func resetNodeJobs() {
	nodeJobs.mu.Lock()
	nodeJobs.pending = make(map[string]bool)
	nodeJobs.order = nil
	nodeJobs.mu.Unlock()
}

const testNodeKey = "85953e404cd421a5d845051a8674d52bc38de2972f2357072ec1e79a79be47ed"

func TestNodeRequestRejectsNonPost(t *testing.T) {
	resetNodeJobs()
	req := httptest.NewRequest(http.MethodGet, nodeRequestPrefix+testNodeKey, nil)
	rec := httptest.NewRecorder()
	handleNodeRequest(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestNodeRequestRejectsInvalidKey(t *testing.T) {
	resetNodeJobs()
	for _, key := range []string{"", "abc", testNodeKey + "ff", "zz" + testNodeKey[2:]} {
		req := httptest.NewRequest(http.MethodPost, nodeRequestPrefix+key, nil)
		rec := httptest.NewRecorder()
		handleNodeRequest(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("key %q: status = %d, want %d", key, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestNodeRequestQueuesAndCoalesces(t *testing.T) {
	resetNodeJobs()
	req := httptest.NewRequest(http.MethodPost, nodeRequestPrefix+testNodeKey, nil)
	rec := httptest.NewRecorder()
	handleNodeRequest(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("first request status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	var first map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	if first["status"] != "queued" {
		t.Fatalf("first status = %q, want queued", first["status"])
	}

	rec = httptest.NewRecorder()
	handleNodeRequest(rec, httptest.NewRequest(http.MethodPost, nodeRequestPrefix+testNodeKey, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("duplicate request status = %d, want %d", rec.Code, http.StatusOK)
	}
	var second map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &second); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	if second["status"] != "coalesced" {
		t.Fatalf("duplicate status = %q, want coalesced", second["status"])
	}
	if len(nodeJobs.order) != 1 {
		t.Fatalf("queue length = %d, want 1", len(nodeJobs.order))
	}
}

func TestNodeRequestQueueIsBounded(t *testing.T) {
	resetNodeJobs()
	for i := 0; i < nodeQueueLimit; i++ {
		key := fmt.Sprintf("%064x", i)
		rec := httptest.NewRecorder()
		handleNodeRequest(rec, httptest.NewRequest(http.MethodPost, nodeRequestPrefix+key, nil))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("fill %d: status = %d", i, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	handleNodeRequest(rec, httptest.NewRequest(http.MethodPost, nodeRequestPrefix+fmt.Sprintf("%064x", nodeQueueLimit), nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("overflow status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
}

func TestLastJSONLine(t *testing.T) {
	if got := lastJSONLine("log line\n{\"state\":\"available\"}\n"); got != `{"state":"available"}` {
		t.Fatalf("got %q", got)
	}
	if got := lastJSONLine("no json here"); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}
