package inapp

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"notification-service/internal/channels"
)

var (
	connections = promauto.NewGauge(prometheus.GaugeOpts{Name: "inapp_connections", Help: "Open WebSocket connections."})
	delivered   = promauto.NewCounter(prometheus.CounterOpts{Name: "inapp_pushed_total", Help: "Messages pushed to live connections."})
	dropped     = promauto.NewCounter(prometheus.CounterOpts{Name: "inapp_dropped_total", Help: "Messages dropped for slow connections."})
)

type Conn struct {
	UserID string
	Send   chan []byte
}

type Hub struct {
	ps  *redis.PubSub
	log *slog.Logger

	mu     sync.Mutex
	users  map[string]map[*Conn]struct{}
	shards map[string]int
}

// NewHub subscribes lazily to the sharded channels of users that are connected to this node.
func NewHub(rdb *redis.Client, log *slog.Logger) *Hub {
	return &Hub{ps: rdb.Subscribe(context.Background()), log: log, users: map[string]map[*Conn]struct{}{}, shards: map[string]int{}}
}

// Register attaches a connection and subscribes to its user's shard channel when first needed.
func (h *Hub) Register(ctx context.Context, c *Conn) error {
	ch := channels.InAppChannel(c.UserID)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.shards[ch] == 0 {
		if err := h.ps.Subscribe(ctx, ch); err != nil {
			return err
		}
	}
	h.shards[ch]++
	if h.users[c.UserID] == nil {
		h.users[c.UserID] = map[*Conn]struct{}{}
	}
	h.users[c.UserID][c] = struct{}{}
	connections.Inc()
	return nil
}

// Unregister detaches a connection and drops the shard subscription when no local user needs it.
func (h *Hub) Unregister(c *Conn) {
	ch := channels.InAppChannel(c.UserID)
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.users[c.UserID][c]; !ok {
		return
	}
	delete(h.users[c.UserID], c)
	if len(h.users[c.UserID]) == 0 {
		delete(h.users, c.UserID)
	}
	connections.Dec()
	if h.shards[ch]--; h.shards[ch] <= 0 {
		delete(h.shards, ch)
		_ = h.ps.Unsubscribe(context.Background(), ch)
	}
}

// Run fans incoming Redis messages out to the matching local connections until ctx ends.
func (h *Hub) Run(ctx context.Context) {
	msgs := h.ps.Channel()
	for {
		select {
		case <-ctx.Done():
			_ = h.ps.Close()
			return
		case m, ok := <-msgs:
			if !ok {
				return
			}
			var p struct {
				UserID string `json:"user_id"`
			}
			if json.Unmarshal([]byte(m.Payload), &p) != nil {
				continue
			}
			h.deliver(p.UserID, []byte(m.Payload))
		}
	}
}

func (h *Hub) deliver(userID string, payload []byte) {
	h.mu.Lock()
	conns := make([]*Conn, 0, len(h.users[userID]))
	for c := range h.users[userID] {
		conns = append(conns, c)
	}
	h.mu.Unlock()
	for _, c := range conns {
		select {
		case c.Send <- payload:
			delivered.Inc()
		default:
			dropped.Inc()
		}
	}
}
