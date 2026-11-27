package corescope

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRepeaterInputsRequireEveryPageInOneVisibilityGeneration(t *testing.T) {
	for _, inconsistent := range []bool{false, true} {
		name := "stable"
		if inconsistent {
			name = "authority changes"
		}
		t.Run(name, func(t *testing.T) {
			a := strings.Repeat("A", 64)
			b := strings.Repeat("B", 64)
			legacy := strings.Repeat("C", 64)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/hopreach/api/nodes" {
					t.Errorf("unexpected route %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				q := r.URL.Query()
				res := nodesResponse{Nodes: []Node{{PublicKey: a}}, Total: 2, VisibilityGeneration: 29}
				if q.Get("projection") == "" {
					res = nodesResponse{Nodes: []Node{{PublicKey: legacy}}, Total: 1}
				} else if q.Get("offset") == "1" {
					if q.Get("generation") != "29" {
						t.Error("subsequent page did not carry initial generation")
					}
					res.Nodes = []Node{{PublicKey: b}}
					if inconsistent {
						res.VisibilityGeneration++
					}
				}
				_ = json.NewEncoder(w).Encode(res)
			}))
			defer server.Close()

			nodes, generation, err := NewClient(server.URL+"/hopreach", server.Client()).FetchRepeaterInputs(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if inconsistent {
				if generation != 0 || len(nodes) != 1 || nodes[0].PublicKey != legacy {
					t.Fatalf("expected unproven legacy list, got generation %d, nodes %+v", generation, nodes)
				}
			} else if generation != 29 || len(nodes) != 2 || nodes[1].PublicKey != b {
				t.Fatalf("expected two current pages, got generation %d, nodes %+v", generation, nodes)
			}
		})
	}
}
