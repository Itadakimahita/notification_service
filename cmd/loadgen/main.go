package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type client struct {
	base, key string
	http      *http.Client
}

func (c *client) do(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "ApiKey "+c.key)
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out, nil
}

type sample struct {
	id       string
	channels []string
}

// main drives a constant arrival rate of notifications and verifies that accepted ones reach a terminal state.
func main() {
	url := flag.String("url", "http://localhost:8081", "API base url")
	key := flag.String("key", "demo-key", "API key")
	rps := flag.Int("rate", 500, "target requests per second")
	dur := flag.Duration("duration", 30*time.Second, "test duration")
	users := flag.Int("users", 500, "number of seeded users")
	channels := flag.String("channels", "push,inapp", "channels per notification")
	marketing := flag.Int("marketing-pct", 0, "percent of requests sent as low-priority marketing")
	maxInflight := flag.Int("max-inflight", 2000, "cap on concurrent requests")
	verifyN := flag.Int("verify", 2000, "sampled notifications to verify (0 disables)")
	verifyWait := flag.Duration("verify-timeout", 90*time.Second, "how long to wait for terminal states")
	flag.Parse()

	c := &client{base: *url, key: *key, http: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		MaxIdleConns: 2048, MaxIdleConnsPerHost: 2048, IdleConnTimeout: 90 * time.Second}}}
	ctx := context.Background()
	chs := strings.Split(*channels, ",")

	fmt.Printf("seeding %d users and templates...\n", *users)
	seed(ctx, c, *users, chs)

	var (
		mu        sync.Mutex
		latencies []time.Duration
		accepted  []sample
		sent      atomic.Int64
		failed    atomic.Int64
		statuses  sync.Map
		dropped   atomic.Int64
	)
	sem := make(chan struct{}, *maxInflight)
	var wg sync.WaitGroup

	fire := func(i int) {
		defer wg.Done()
		defer func() { <-sem }()
		category, prio := "transactional", "high"
		if rand.IntN(100) < *marketing {
			category, prio = "marketing", "low"
		}
		body := map[string]any{"user_id": fmt.Sprintf("lt-%04d", i%*users), "template_id": "loadtest", "category": category,
			"priority": prio, "channels": chs, "data": map[string]string{"n": fmt.Sprint(i)}}
		start := time.Now()
		code, out, err := c.do(ctx, http.MethodPost, "/v1/notifications", body)
		lat := time.Since(start)
		sent.Add(1)
		if err != nil || code != http.StatusAccepted {
			failed.Add(1)
			k := fmt.Sprint(code)
			if err != nil {
				k = "error"
			}
			v, _ := statuses.LoadOrStore(k, new(atomic.Int64))
			v.(*atomic.Int64).Add(1)
			return
		}
		var r struct {
			ID string `json:"notification_id"`
		}
		_ = json.Unmarshal(out, &r)
		mu.Lock()
		latencies = append(latencies, lat)
		if *verifyN > 0 && len(accepted) < *verifyN {
			accepted = append(accepted, sample{r.ID, chs})
		} else if *verifyN > 0 && rand.IntN(len(latencies)) < *verifyN {
			accepted[rand.IntN(*verifyN)] = sample{r.ID, chs}
		}
		mu.Unlock()
	}

	fmt.Printf("running %d req/s for %s...\n", *rps, *dur)
	begin := time.Now()
	interval := time.Second / time.Duration(*rps)
	for i := 0; ; i++ {
		due := begin.Add(time.Duration(i) * interval)
		if due.Sub(begin) >= *dur {
			break
		}
		if d := time.Until(due); d > 0 {
			time.Sleep(d)
		}
		select {
		case sem <- struct{}{}:
			wg.Add(1)
			go fire(i)
		default:
			dropped.Add(1)
		}
	}
	wg.Wait()
	elapsed := time.Since(begin)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[min(len(latencies)-1, int(float64(len(latencies))*p))]
	}
	fmt.Println("\n== accept path ==")
	fmt.Printf("target=%d/s achieved=%.0f/s requests=%d accepted=%d failed=%d client_dropped=%d\n",
		*rps, float64(len(latencies))/elapsed.Seconds(), sent.Load(), len(latencies), failed.Load(), dropped.Load())
	fmt.Printf("latency p50=%s p95=%s p99=%s max=%s\n", pct(.5).Round(time.Microsecond), pct(.95).Round(time.Microsecond), pct(.99).Round(time.Microsecond), pct(1).Round(time.Microsecond))
	statuses.Range(func(k, v any) bool { fmt.Printf("non-202 %v: %d\n", k, v.(*atomic.Int64).Load()); return true })

	if *verifyN > 0 {
		verify(ctx, c, accepted, *verifyWait)
	}
	if failed.Load() > 0 {
		os.Exit(1)
	}
}

func seed(ctx context.Context, c *client, users int, chs []string) {
	for _, ch := range chs {
		c.do(ctx, http.MethodPost, "/v1/templates", map[string]string{"template_id": "loadtest", "channel": ch, "subject": "Load {{.n}}", "body": "Load test message {{.n}}"})
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 64)
	for i := range users {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			id := fmt.Sprintf("lt-%04d", i)
			c.do(ctx, http.MethodPut, "/v1/users/"+id, map[string]string{"locale": "en", "timezone": "UTC", "email": id + "@example.com"})
			c.do(ctx, http.MethodPost, "/v1/users/"+id+"/devices", map[string]string{"token": "lt-tok-" + id, "platform": "ios"})
		}()
	}
	wg.Wait()
}

func terminal(ch, status string) bool {
	switch status {
	case "DELIVERED", "FAILED", "SUPPRESSED", "EXPIRED":
		return true
	case "SENT":
		return ch == "email"
	}
	return false
}

// verify polls sampled notifications until every requested channel is terminal or the deadline passes.
func verify(ctx context.Context, c *client, sampled []sample, wait time.Duration) {
	fmt.Printf("\n== delivery verification (%d sampled, up to %s) ==\n", len(sampled), wait)
	deadline := time.Now().Add(wait)
	pending := sampled
	final := map[string]int{}
	start := time.Now()
	for len(pending) > 0 && time.Now().Before(deadline) {
		var next []sample
		var mu sync.Mutex
		var wg sync.WaitGroup
		sem := make(chan struct{}, 64)
		for _, s := range pending {
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				code, out, err := c.do(ctx, http.MethodGet, "/v1/notifications/"+s.id, nil)
				done := false
				var res []string
				if err == nil && code == http.StatusOK {
					var n struct {
						ChannelStatus map[string]string `json:"channel_status"`
					}
					_ = json.Unmarshal(out, &n)
					done = len(n.ChannelStatus) >= len(s.channels)
					for ch, st := range n.ChannelStatus {
						if !terminal(ch, st) {
							done = false
						}
						res = append(res, ch+"="+st)
					}
				}
				mu.Lock()
				defer mu.Unlock()
				if done {
					sort.Strings(res)
					final[strings.Join(res, ",")]++
				} else {
					next = append(next, s)
				}
			}()
		}
		wg.Wait()
		pending = next
		if len(pending) > 0 {
			time.Sleep(2 * time.Second)
		}
	}
	keys := make([]string, 0, len(final))
	for k := range final {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-40s %d\n", k, final[k])
	}
	fmt.Printf("terminal=%d unaccounted=%d (waited %s)\n", len(sampled)-len(pending), len(pending), time.Since(start).Round(time.Second))
	if len(pending) > 0 {
		os.Exit(2)
	}
}
