package ratelimit

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestLimiterEnforcesRateAcrossInstances(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	key := fmt.Sprintf("rl:test:%d", time.Now().UnixNano())
	a, b := New(rdb, key, 200, 20, 5), New(rdb, key, 200, 20, 5)

	start := time.Now()
	var wg sync.WaitGroup
	for _, l := range []*Limiter{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 60 {
				require.NoError(t, l.Wait(context.Background()))
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// 120 tokens, burst 20, 200/s: at least (120-20)/200 = 0.5s, and not wildly slower.
	require.GreaterOrEqual(t, elapsed, 450*time.Millisecond)
	require.Less(t, elapsed, 3*time.Second)
}

func TestLimiterHonorsContext(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	l := New(rdb, fmt.Sprintf("rl:test:%d", time.Now().UnixNano()), 1, 1, 1)
	require.NoError(t, l.Wait(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Error(t, l.Wait(ctx))
}
