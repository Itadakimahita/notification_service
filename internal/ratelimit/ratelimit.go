package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var script = redis.NewScript(`
local rate, burst, want = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens, ts = tonumber(d[1]), tonumber(d[2])
if tokens == nil then tokens = burst; ts = now end
tokens = math.min(burst, tokens + (now - ts) * rate / 1000)
local granted = math.min(want, math.floor(tokens))
tokens = tokens - granted
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 60000)
local wait = 0
if granted == 0 then wait = math.ceil((1 - tokens) * 1000 / rate) end
return {granted, wait}
`)

type Limiter struct {
	rdb   *redis.Client
	key   string
	rate  float64
	burst int
	chunk int

	mu    sync.Mutex
	local int
}

// New creates a distributed token bucket that leases tokens from Redis in chunks to cut round trips.
func New(rdb *redis.Client, key string, rate float64, burst, chunk int) *Limiter {
	return &Limiter{rdb: rdb, key: key, rate: rate, burst: burst, chunk: chunk}
}

// Wait blocks until one token is available or ctx ends.
func (l *Limiter) Wait(ctx context.Context) error {
	for {
		l.mu.Lock()
		if l.local > 0 {
			l.local--
			l.mu.Unlock()
			return nil
		}
		res, err := script.Run(ctx, l.rdb, []string{l.key}, l.rate, l.burst, l.chunk).Int64Slice()
		if err == nil && len(res) == 2 && res[0] > 0 {
			l.local += int(res[0]) - 1
			l.mu.Unlock()
			return nil
		}
		l.mu.Unlock()
		if err != nil {
			return err
		}
		wait := time.Duration(res[1]) * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(max(wait, time.Millisecond)):
		}
	}
}
