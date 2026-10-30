package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	pb "notification-service/internal/events/notification/v1"
	"notification-service/internal/store/dynamo"
)

const pageSize = 500

var (
	emitted    = promauto.NewCounter(prometheus.CounterOpts{Name: "scheduler_emitted_total", Help: "Due requests re-emitted to Kafka."})
	ownedShard = promauto.NewGauge(prometheus.GaugeOpts{Name: "scheduler_owned_shards", Help: "Schedule shards currently leased."})
)

type Store interface {
	Due(ctx context.Context, minute int64, shard int, nowUnix int64, limit int32) ([]dynamo.ScheduledItem, error)
	Delete(ctx context.Context, items []dynamo.ScheduledItem) error
}

type Leases interface {
	Acquire(ctx context.Context, name, owner string) (dynamo.Lease, bool, error)
	Checkpoint(ctx context.Context, name, owner, checkpoint string, done bool) (bool, error)
}

type Scheduler struct {
	Store   Store
	Leases  Leases
	Kafka   *kgo.Client
	Topic   string
	Owner   string
	Log     *slog.Logger
	Tick    time.Duration
	Catchup time.Duration
	Now     func() time.Time
}

// Run sweeps due buckets of every shard it can lease, once per tick, until ctx ends.
func (s *Scheduler) Run(ctx context.Context) error {
	tick := time.NewTicker(s.Tick)
	defer tick.Stop()
	for {
		if err := s.sweep(ctx); err != nil && ctx.Err() == nil {
			s.Log.Error("sweep failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func (s *Scheduler) sweep(ctx context.Context) error {
	var owned int64
	g, gctx := errgroup.WithContext(ctx)
	results := make(chan bool, dynamo.ScheduleShards)
	for shard := range dynamo.ScheduleShards {
		g.Go(func() error {
			ok, err := s.sweepShard(gctx, shard)
			results <- ok
			return err
		})
	}
	err := g.Wait()
	close(results)
	for ok := range results {
		if ok {
			owned++
		}
	}
	ownedShard.Set(float64(owned))
	return err
}

// sweepShard drains every bucket from the checkpoint to the current minute; it reports whether the lease is held.
func (s *Scheduler) sweepShard(ctx context.Context, shard int) (bool, error) {
	name := fmt.Sprintf("scheduler#%02d", shard)
	lease, ok, err := s.Leases.Acquire(ctx, name, s.Owner)
	if err != nil || !ok {
		return false, err
	}

	now := s.Now()
	cur := now.Unix() / 60
	start := (now.Add(-s.Catchup)).Unix() / 60
	if cp, err := strconv.ParseInt(lease.Checkpoint, 10, 64); err == nil && cp > start {
		start = cp
	}
	for m := start; m <= cur; m++ {
		if err := s.drain(ctx, m, shard, now.Unix()); err != nil {
			return true, err
		}
	}
	// one minute of margin keeps late writes from clock skew inside the scanned range
	if _, err := s.Leases.Checkpoint(ctx, name, s.Owner, strconv.FormatInt(max(start, cur-1), 10), false); err != nil {
		return true, err
	}
	return true, nil
}

func (s *Scheduler) drain(ctx context.Context, minute int64, shard int, nowUnix int64) error {
	for {
		items, err := s.Store.Due(ctx, minute, shard, nowUnix, pageSize)
		if err != nil || len(items) == 0 {
			return err
		}
		recs := make([]*kgo.Record, 0, len(items))
		for _, it := range items {
			var req pb.NotificationRequested
			if err := proto.Unmarshal(it.Payload, &req); err != nil {
				s.Log.Error("dropping undecodable scheduled item", "pk", it.PK, "sk", it.SK, "err", err)
				continue
			}
			recs = append(recs, &kgo.Record{
				Topic:   s.Topic,
				Key:     []byte(req.UserId),
				Value:   it.Payload,
				Headers: []kgo.RecordHeader{{Key: "notification_id", Value: []byte(req.NotificationId)}},
			})
		}
		if len(recs) > 0 {
			if err := s.Kafka.ProduceSync(ctx, recs...).FirstErr(); err != nil {
				return err
			}
			emitted.Add(float64(len(recs)))
		}
		delCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err = s.Store.Delete(delCtx, items)
		cancel()
		if err != nil {
			return err
		}
		if len(items) < pageSize {
			return nil
		}
	}
}
