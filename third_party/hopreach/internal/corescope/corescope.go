// Package corescope is a client for a CoreScope instance's public API:
// listing repeater nodes and fetching each one's observed reach data (used
// by internal/calibration).
package corescope

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

// pageLimit is the page size used when paginating GET /api/nodes.
const pageLimit = 500

// Node mirrors one entry of CoreScope's GET /api/nodes response.
type Node struct {
	PublicKey     string   `json:"public_key"`
	Name          *string  `json:"name"`
	Role          string   `json:"role"`
	Lat           *float64 `json:"lat"`
	Lon           *float64 `json:"lon"`
	LastHeard     *string  `json:"last_heard"`
	FirstSeen     *string  `json:"first_seen"`
	AdvertCount   *int     `json:"advert_count"`
	RelayCount1h  *int     `json:"relay_count_1h"`
	RelayCount24h *int     `json:"relay_count_24h"`
	HashSize      *int     `json:"hash_size"`
	DefaultScope  *string  `json:"default_scope"`
}

type nodesResponse struct {
	Nodes                []Node `json:"nodes"`
	Total                int    `json:"total"`
	VisibilityGeneration int64  `json:"visibility_generation"`
}

// ReachLink mirrors one entry of CoreScope's GET /api/nodes/:pubkey/reach
// "links" array — real observed relay-path evidence between two repeaters,
// not a prediction. Bottleneck is the weaker direction's observation
// count, which CoreScope itself already frames as "how sure are we this
// link is real and mutually usable" — used directly as a confidence weight
// by internal/calibration.
type ReachLink struct {
	Pubkey     string   `json:"pubkey"`
	Name       string   `json:"name"`
	Lat        *float64 `json:"lat"`
	Lon        *float64 `json:"lon"`
	Bottleneck int      `json:"bottleneck"`
	Bidir      bool     `json:"bidir"`
}

type reachResponse struct {
	Links []ReachLink `json:"links"`
}

type bulkReachRequest struct {
	PublicKeys []string `json:"public_keys"`
	Days       int      `json:"days"`
}

type bulkReachResponse struct {
	LinksByPublicKey map[string][]ReachLink `json:"links_by_public_key"`
}

const maxBulkReachResponseBytes = 64 * 1024 * 1024

// Client talks to one CoreScope instance.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// NewClient returns a Client for baseURL. A nil httpClient uses
// http.DefaultClient.
func NewClient(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{BaseURL: baseURL, HTTP: httpClient}
}

// FetchRepeaters fetches every role=repeater node, paginating as needed.
func (c *Client) FetchRepeaters(ctx context.Context) ([]Node, error) {
	nodes, _, err := c.fetchRepeaters(ctx, false)
	return nodes, err
}

// FetchRepeaterInputs selects the generation-bound UK Mesh production input
// when offered by the private adapter. Legacy CoreScope serves the old raw
// products; those inputs cannot attest any public-projection tiles.
func (c *Client) FetchRepeaterInputs(ctx context.Context) ([]Node, int64, error) {
	endpoint, err := url.Parse(c.BaseURL)
	if err != nil || strings.TrimSuffix(endpoint.Path, "/") != "/hopreach" {
		nodes, err := c.FetchRepeaters(ctx)
		return nodes, 0, err
	}
	nodes, generation, err := c.fetchRepeaters(ctx, true)
	if err == nil {
		return nodes, generation, nil
	}
	nodes, err = c.FetchRepeaters(ctx)
	return nodes, 0, err
}

func (c *Client) fetchRepeaters(ctx context.Context, projected bool) ([]Node, int64, error) {
	var nodes []Node
	var generation int64
	total := -1
	lastKey := ""
	offset := 0
	for {
		url := fmt.Sprintf("%s/api/nodes?role=repeater&limit=%d&offset=%d", c.BaseURL, pageLimit, offset)
		if projected {
			url += "&projection=public"
			if generation > 0 {
				url += fmt.Sprintf("&generation=%d", generation)
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("corescope: building request for %s: %w", url, err)
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return nil, 0, fmt.Errorf("corescope: fetching %s: %w", url, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, 0, fmt.Errorf("corescope: fetching %s: unexpected status %d", url, resp.StatusCode)
		}
		var page nodesResponse
		err = json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if err != nil {
			return nil, 0, fmt.Errorf("corescope: decoding response from %s: %w", url, err)
		}
		if projected {
			if page.VisibilityGeneration < 1 || page.VisibilityGeneration > 9007199254740991 ||
				(generation != 0 && generation != page.VisibilityGeneration) ||
				(total >= 0 && total != page.Total) || page.Total < 0 ||
				offset+len(page.Nodes) > page.Total || (len(page.Nodes) == 0 && offset != page.Total) {
				return nil, 0, fmt.Errorf("incomplete or changed RF visibility snapshot")
			}
			total, generation = page.Total, page.VisibilityGeneration
			for _, node := range page.Nodes {
				if len(node.PublicKey) != 64 || node.PublicKey <= lastKey {
					return nil, 0, fmt.Errorf("unordered or duplicate RF visibility snapshot")
				}
				lastKey = node.PublicKey
			}
		}
		nodes = append(nodes, page.Nodes...)
		offset += len(page.Nodes)
		if len(page.Nodes) == 0 || offset >= page.Total {
			break
		}
	}
	return nodes, generation, nil
}

// FetchReach fetches one node's observed reach data for the given lookback
// window (days).
func (c *Client) FetchReach(ctx context.Context, pubkey string, days int) ([]ReachLink, error) {
	url := fmt.Sprintf("%s/api/nodes/%s/reach?days=%d", c.BaseURL, pubkey, days)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("corescope: building request for %s: %w", url, err)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var parsed reachResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, err
	}
	return parsed.Links, nil
}

// FetchReachBulk loads observed-link calibration evidence in one bounded
// request. UK Mesh provides this optional endpoint for large deployments;
// callers retain FetchReach as a compatibility fallback for CoreScope servers
// which do not implement it.
func (c *Client) FetchReachBulk(ctx context.Context, nodes []Node, days int) (map[string][]ReachLink, error) {
	publicKeys := make([]string, len(nodes))
	for i := range nodes {
		publicKeys[i] = nodes[i].PublicKey
	}
	body, err := json.Marshal(bulkReachRequest{PublicKeys: publicKeys, Days: days})
	if err != nil {
		return nil, fmt.Errorf("corescope: encoding bulk reach request: %w", err)
	}
	url := fmt.Sprintf("%s/api/reach/bulk", c.BaseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("corescope: building request for %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("corescope: fetching %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("corescope: fetching %s: unexpected status %d", url, resp.StatusCode)
	}
	limited := io.LimitReader(resp.Body, maxBulkReachResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("corescope: reading %s: %w", url, err)
	}
	if len(data) > maxBulkReachResponseBytes {
		return nil, fmt.Errorf("corescope: bulk reach response exceeds %d bytes", maxBulkReachResponseBytes)
	}
	var parsed bulkReachResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("corescope: decoding response from %s: %w", url, err)
	}
	if parsed.LinksByPublicKey == nil {
		return nil, fmt.Errorf("corescope: bulk reach response has no links_by_public_key")
	}
	return parsed.LinksByPublicKey, nil
}

// FetchAllReach fetches reach data for every node concurrently, keyed by
// public key. A node whose fetch fails is simply omitted — treated as "no
// evidence available" for calibration — rather than aborting the whole
// batch, consistent with this tool's graceful-degradation approach
// elsewhere. progress is called with (done, total) as each node resolves.
func FetchAllReach(ctx context.Context, c *Client, nodes []Node, days int, progress func(done, total int)) map[string][]ReachLink {
	if results, err := c.FetchReachBulk(ctx, nodes, days); err == nil {
		if progress != nil {
			progress(len(nodes), len(nodes))
		}
		return results
	} else {
		log.Printf("corescope: bulk reach unavailable, using per-node compatibility fallback: %v", err)
	}

	results := make(map[string][]ReachLink, len(nodes))
	var mu sync.Mutex

	jobs := make(chan string, len(nodes))
	for _, n := range nodes {
		jobs <- n.PublicKey
	}
	close(jobs)

	var wg sync.WaitGroup
	const workers = 12
	done := 0
	total := len(nodes)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pubkey := range jobs {
				links, err := c.FetchReach(ctx, pubkey, days)
				mu.Lock()
				if err != nil {
					log.Printf("corescope: reach fetch failed for %s: %v", pubkey, err)
				} else {
					results[pubkey] = links
				}
				done++
				if progress != nil {
					progress(done, total)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return results
}
