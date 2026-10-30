package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"notification-service/internal/config"
	pb "notification-service/internal/events/notification/v1"
	"notification-service/internal/kafkax"
	"notification-service/internal/store/dynamo"
)

func TestSchedulerEmitsOnlyDueItemsOnceAndCatchesUp(t *testing.T) {
	endpoint, brokers := os.Getenv("DYNAMO_ENDPOINT"), os.Getenv("KAFKA_BROKERS")
	if endpoint == "" || brokers == "" {
		t.Skip("DYNAMO_ENDPOINT and KAFKA_BROKERS required")
	}
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	dc := config.Dynamo{Endpoint: endpoint, Region: "us-east-1", TablePrefix: fmt.Sprintf("s%d_", suffix)}
	awsCfg, err := dynamo.LoadAWS(ctx, dc)
	require.NoError(t, err)
	db := dynamo.NewClient(awsCfg, dc)
	tables := dynamo.NewTables(dc.TablePrefix)
	require.NoError(t, dynamo.EnsureTables(ctx, db, tables))
	sched := dynamo.NewSchedule(db, tables)

	kc, err := kafkax.NewClient(strings.Split(brokers, ","))
	require.NoError(t, err)
	t.Cleanup(kc.Close)
	topic := fmt.Sprintf("test.sched.%d", suffix)
	require.NoError(t, kafkax.EnsureTopics(ctx, kadm.NewClient(kc), []kafkax.TopicSpec{{Name: topic, Partitions: 3, Retention: time.Hour}}, 3))

	now := time.Now()
	put := func(id string, due time.Time) {
		b, err := proto.Marshal(&pb.NotificationRequested{NotificationId: id, UserId: "u-" + id, Released: true})
		require.NoError(t, err)
		require.NoError(t, sched.Put(ctx, id, "push", due, b))
	}
	put("due-now", now.Add(-time.Second))
	put("due-earlier-minute", now.Add(-3*time.Minute))
	put("due-3h-ago-catchup", now.Add(-3*time.Hour))
	futureDue := now.Add(time.Hour)
	put("future", futureDue)

	run := func(owner string) context.CancelFunc {
		rctx, cancel := context.WithCancel(ctx)
		s := &Scheduler{Store: sched, Leases: dynamo.NewLeases(db, tables, 2*time.Second), Kafka: kc, Topic: topic,
			Owner: owner, Log: slog.Default(), Tick: 200 * time.Millisecond, Catchup: 24 * time.Hour, Now: time.Now}
		done := make(chan struct{})
		go func() { defer close(done); _ = s.Run(rctx) }()
		return func() { cancel(); <-done }
	}
	stop := run("a")

	consumer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	t.Cleanup(consumer.Close)

	seen := map[string]int{}
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for len(seen) < 3 {
		fetches := consumer.PollFetches(deadline)
		require.NoError(t, deadline.Err(), "got %v", seen)
		fetches.EachRecord(func(r *kgo.Record) {
			var ev pb.NotificationRequested
			require.NoError(t, proto.Unmarshal(r.Value, &ev))
			require.True(t, ev.Released)
			require.Equal(t, "u-"+ev.NotificationId, string(r.Key))
			seen[ev.NotificationId]++
		})
	}
	stop()

	// restart a second instance: nothing re-emitted, future item still waiting
	stop = run("b")
	defer stop()
	drain, cancelDrain := context.WithTimeout(ctx, 4*time.Second)
	defer cancelDrain()
	consumer.PollFetches(drain).EachRecord(func(r *kgo.Record) {
		var ev pb.NotificationRequested
		require.NoError(t, proto.Unmarshal(r.Value, &ev))
		seen[ev.NotificationId]++
	})
	require.Equal(t, map[string]int{"due-now": 1, "due-earlier-minute": 1, "due-3h-ago-catchup": 1}, seen)

	total := 0
	for shard := 0; shard < dynamo.ScheduleShards; shard++ {
		items, err := sched.Due(ctx, futureDue.Unix()/60, shard, futureDue.Add(time.Hour).Unix(), 10)
		require.NoError(t, err)
		total += len(items)
	}
	require.Equal(t, 1, total, "future item must remain scheduled")
}
