package router

import (
	"context"
	"log/slog"
	"strings"
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
	processed = promauto.NewCounterVec(prometheus.CounterOpts{Name: "router_processed_total", Help: "Routed requests by result."}, []string{"result"})
	routeTime = promauto.NewHistogram(prometheus.HistogramOpts{Name: "router_route_seconds", Help: "Time to route one request.", Buckets: prometheus.DefBuckets})
)

type ScheduleStore interface {
	Put(ctx context.Context, notificationID, variant string, due time.Time, payload []byte) error
}

type Consumer struct {
	Router      *Router
	Client      *kgo.Client
	Schedule    ScheduleStore
	Log         *slog.Logger
	Parallelism int
	BatchSize   int
}

// Run consumes requested events, routes them in parallel batches and commits after outputs are produced.
func (c *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		fetches := c.Client.PollRecords(ctx, c.BatchSize)
		if ctx.Err() != nil {
			break
		}
		fetches.EachError(func(t string, p int32, err error) { c.Log.Error("fetch error", "topic", t, "partition", p, "err", err) })

		recs := fetches.Records()
		if len(recs) == 0 {
			c.Client.AllowRebalance()
			continue
		}
		if err := c.handleBatch(ctx, recs); err != nil {
			c.Client.AllowRebalance()
			return err
		}
		if err := c.Client.CommitRecords(ctx, recs...); err != nil && ctx.Err() == nil {
			c.Log.Error("commit failed", "err", err)
		}
		c.Client.AllowRebalance()
	}
	return nil
}

func (c *Consumer) handleBatch(ctx context.Context, recs []*kgo.Record) error {
	var (
		mu      sync.Mutex
		outputs []*kgo.Record
		wg      sync.WaitGroup
		sem     = make(chan struct{}, c.Parallelism)
	)
	for _, rec := range recs {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			produced := c.handleRecord(ctx, rec)
			mu.Lock()
			outputs = append(outputs, produced...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return nil
	}
	if len(outputs) == 0 {
		return nil
	}
	return c.Client.ProduceSync(ctx, outputs...).FirstErr()
}

// handleRecord routes one record, retrying infrastructure errors until they clear.
func (c *Consumer) handleRecord(ctx context.Context, rec *kgo.Record) []*kgo.Record {
	var req pb.NotificationRequested
	if err := proto.Unmarshal(rec.Value, &req); err != nil {
		processed.WithLabelValues("poison").Inc()
		c.Log.Error("undecodable record", "partition", rec.Partition, "offset", rec.Offset, "err", err)
		return []*kgo.Record{{Topic: kafkax.TopicDLQRouter, Key: rec.Key, Value: rec.Value,
			Headers: append(rec.Headers, kgo.RecordHeader{Key: "error", Value: []byte(err.Error())})}}
	}

	var out Outcome
	for backoff := 100 * time.Millisecond; ; backoff = min(backoff*2, 5*time.Second) {
		start := time.Now()
		var err error
		out, err = c.Router.Route(ctx, &req)
		routeTime.Observe(time.Since(start).Seconds())
		if err == nil {
			err = c.defer_(ctx, &req, out.Deferred)
		}
		if err == nil {
			break
		}
		processed.WithLabelValues("retry").Inc()
		c.Log.Warn("routing failed, retrying", "notification_id", req.NotificationId, "err", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
	}
	processed.WithLabelValues("ok").Inc()

	var produced []*kgo.Record
	for _, d := range out.Dispatch {
		val, _ := proto.Marshal(d)
		produced = append(produced, &kgo.Record{
			Topic:   kafkax.DispatchTopic(domain.Channel(d.Channel), domain.Priority(d.Priority)),
			Key:     []byte(d.UserId),
			Value:   val,
			Headers: []kgo.RecordHeader{{Key: "notification_id", Value: []byte(d.NotificationId)}},
		})
	}
	for _, s := range out.Status {
		val, _ := proto.Marshal(s)
		produced = append(produced, &kgo.Record{Topic: kafkax.TopicStatus, Key: []byte(s.NotificationId), Value: val})
	}
	return produced
}

// defer_ stores deferred deliveries so the scheduler can re-emit them when due.
func (c *Consumer) defer_(ctx context.Context, req *pb.NotificationRequested, defs []Deferral) error {
	for _, d := range defs {
		released := proto.Clone(req).(*pb.NotificationRequested)
		released.Channels, released.Released, released.SendAt = d.Channels, true, 0
		payload, err := proto.Marshal(released)
		if err != nil {
			return err
		}
		if err := c.Schedule.Put(ctx, req.NotificationId, strings.Join(d.Channels, ","), d.At, payload); err != nil {
			return err
		}
	}
	return nil
}
