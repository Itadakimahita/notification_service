package status

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"notification-service/internal/domain"
	pb "notification-service/internal/events/notification/v1"
	"notification-service/internal/kafkax"
)

var (
	received = promauto.NewCounter(prometheus.CounterOpts{Name: "status_events_total", Help: "Status events consumed."})
	applied  = promauto.NewCounter(prometheus.CounterOpts{Name: "status_updates_total", Help: "DynamoDB status updates after coalescing."})
)

type Applier interface {
	Apply(ctx context.Context, nid, channel string, st domain.Status, detail string) error
}

type Consumer struct {
	Store       Applier
	Client      *kgo.Client
	Log         *slog.Logger
	BatchSize   int
	Parallelism int
}

// Run consumes status events and applies them to the notification records until ctx ends.
func (c *Consumer) Run(ctx context.Context) error {
	return kafkax.RunBatches(ctx, c.Client, c.BatchSize, c.handle, c.Log)
}

type key struct{ nid, channel string }

// Coalesce keeps only the highest-ranked event per notification and channel in a batch.
func Coalesce(events []*pb.StatusEvent) []*pb.StatusEvent {
	best := map[key]*pb.StatusEvent{}
	var order []key
	for _, e := range events {
		k := key{e.NotificationId, e.Channel}
		cur, ok := best[k]
		if !ok {
			order = append(order, k)
		}
		if !ok || domain.Status(e.Status).Rank() >= domain.Status(cur.Status).Rank() {
			best[k] = e
		}
	}
	out := make([]*pb.StatusEvent, 0, len(order))
	for _, k := range order {
		out = append(out, best[k])
	}
	return out
}

func (c *Consumer) handle(ctx context.Context, recs []*kgo.Record) ([]*kgo.Record, error) {
	events := make([]*pb.StatusEvent, 0, len(recs))
	for _, r := range recs {
		var e pb.StatusEvent
		if err := proto.Unmarshal(r.Value, &e); err != nil {
			c.Log.Error("undecodable status event", "offset", r.Offset, "err", err)
			continue
		}
		events = append(events, &e)
	}
	received.Add(float64(len(events)))
	merged := Coalesce(events)

	var wg sync.WaitGroup
	sem := make(chan struct{}, c.Parallelism)
	for _, e := range merged {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			c.applyWithRetry(ctx, e)
		}()
	}
	wg.Wait()
	applied.Add(float64(len(merged)))
	return nil, nil
}

// applyWithRetry retries infrastructure failures until they clear or ctx ends.
func (c *Consumer) applyWithRetry(ctx context.Context, e *pb.StatusEvent) {
	for backoff := 100 * time.Millisecond; ctx.Err() == nil; backoff = min(backoff*2, 5*time.Second) {
		err := c.Store.Apply(ctx, e.NotificationId, e.Channel, domain.Status(e.Status), e.Detail)
		if err == nil {
			return
		}
		c.Log.Warn("status update failed, retrying", "notification_id", e.NotificationId, "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
	}
}
