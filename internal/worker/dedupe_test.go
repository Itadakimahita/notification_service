package worker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisDedupeStates(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	d := RedisDedupe{RDB: rdb}
	ctx := context.Background()
	key := fmt.Sprintf("sent:test:%d", time.Now().UnixNano())

	st, err := d.Claim(ctx, key)
	require.NoError(t, err)
	require.Equal(t, Claimed, st)
	st, _ = d.Claim(ctx, key)
	require.Equal(t, InFlight, st)

	require.NoError(t, d.MarkSent(ctx, key))
	st, _ = d.Claim(ctx, key)
	require.Equal(t, AlreadySent, st)

	require.NoError(t, d.Release(ctx, key))
	st, _ = d.Claim(ctx, key)
	require.Equal(t, Claimed, st)
}
