package inapp

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"notification-service/internal/channels"
)

func TestGatewayPushesOnlyToTheTargetUser(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	hub := NewHub(rdb, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)
	srv := httptest.NewServer((&Server{Hub: hub, Keys: map[string]bool{"k": true}}).Handler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/ws?user_id="

	dial := func(user string) *websocket.Conn {
		c, _, err := websocket.Dial(ctx, url+user, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"ApiKey k"}}})
		require.NoError(t, err)
		return c
	}
	alice, bob := dial("alice"), dial("bob")
	defer alice.CloseNow()
	defer bob.CloseNow()
	time.Sleep(300 * time.Millisecond)

	payload, _ := json.Marshal(map[string]any{"user_id": "alice", "title": "hi"})
	require.NoError(t, rdb.Publish(ctx, channels.InAppChannel("alice"), payload).Err())

	rctx, rcancel := context.WithTimeout(ctx, 3*time.Second)
	defer rcancel()
	_, msg, err := alice.Read(rctx)
	require.NoError(t, err)
	require.Contains(t, string(msg), `"title":"hi"`)

	bctx, bcancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer bcancel()
	_, _, err = bob.Read(bctx)
	require.Error(t, err, "bob must not receive alice's message even if they share a shard")
}

func TestGatewayRejectsBadKeyAndMissingUser(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	srv := httptest.NewServer((&Server{Hub: NewHub(rdb, slog.Default()), Keys: map[string]bool{"k": true}}).Handler())
	defer srv.Close()

	do := func(path, key string) int {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		if key != "" {
			req.Header.Set("Authorization", "ApiKey "+key)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		return resp.StatusCode
	}
	require.Equal(t, 401, do("/v1/ws?user_id=a", ""))
	require.Equal(t, 401, do("/v1/ws?user_id=a", "bad"))
	require.Equal(t, 400, do("/v1/ws", "k"))
}
