// UK Mesh modifications, 2026-08-02. Licensed with HopReach under
// AGPL-3.0 plus Commons Clause.
package corescope

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestFetchAllReachUsesBulkEndpoint(t *testing.T) {
	var bulkCalls atomic.Int32
	var nodeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/reach/bulk" {
			bulkCalls.Add(1)
			var body bulkReachRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Days != 14 || len(body.PublicKeys) != 2 {
				t.Fatalf("unexpected bulk body: %#v", body)
			}
			json.NewEncoder(w).Encode(bulkReachResponse{LinksByPublicKey: map[string][]ReachLink{
				"a": {{Pubkey: "b", Bottleneck: 3, Bidir: true}},
				"b": {{Pubkey: "a", Bottleneck: 3, Bidir: true}},
			}})
			return
		}
		nodeCalls.Add(1)
		http.NotFound(w, r)
	}))
	defer server.Close()
	client := NewClient(server.URL, server.Client())
	got := FetchAllReach(context.Background(), client, []Node{{PublicKey: "a"}, {PublicKey: "b"}}, 14, nil)
	if bulkCalls.Load() != 1 || nodeCalls.Load() != 0 || len(got) != 2 {
		t.Fatalf("bulk=%d node=%d results=%d", bulkCalls.Load(), nodeCalls.Load(), len(got))
	}
}

func TestFetchAllReachFallsBackPerNode(t *testing.T) {
	var bulkCalls atomic.Int32
	var nodeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/reach/bulk" {
			bulkCalls.Add(1)
			http.NotFound(w, r)
			return
		}
		nodeCalls.Add(1)
		json.NewEncoder(w).Encode(reachResponse{Links: []ReachLink{{Pubkey: "peer", Bottleneck: 1}}})
	}))
	defer server.Close()
	client := NewClient(server.URL, server.Client())
	got := FetchAllReach(context.Background(), client, []Node{{PublicKey: "a"}, {PublicKey: "b"}}, 7, nil)
	if bulkCalls.Load() != 1 || nodeCalls.Load() != 2 || len(got) != 2 {
		t.Fatalf("bulk=%d node=%d results=%d", bulkCalls.Load(), nodeCalls.Load(), len(got))
	}
}
