package relay

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
	"notification-service/internal/domain"
	events "notification-service/internal/events/notification/v1"
	"notification-service/internal/kafkax"
	"notification-service/internal/store/dynamo"
)

func startRelay(t *testing.T, r *Relay) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return func() { cancel(); <-done }
}

func TestRelayPublishesOnceAcrossRestart(t *testing.T) {
	endpoint, brokers := os.Getenv("DYNAMO_ENDPOINT"), os.Getenv("KAFKA_BROKERS")
	if endpoint == "" || brokers == "" {
		t.Skip("DYNAMO_ENDPOINT and KAFKA_BROKERS required")
	}
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	dc := config.Dynamo{Endpoint: endpoint, Region: "us-east-1", TablePrefix: fmt.Sprintf("r%d_", suffix)}
	awsCfg, err := dynamo.LoadAWS(ctx, dc)
	require.NoError(t, err)
	db := dynamo.NewClient(awsCfg, dc)
	tables := dynamo.NewTables(dc.TablePrefix)
	require.NoError(t, dynamo.EnsureTables(ctx, db, tables))
	repo := dynamo.NewNotifications(db, tables)

	kc, err := kafkax.NewClient(strings.Split(brokers, ","))
	require.NoError(t, err)
	t.Cleanup(kc.Close)
	topic := fmt.Sprintf("test.requested.%d", suffix)
	require.NoError(t, kafkax.EnsureTopics(ctx, kadm.NewClient(kc), []kafkax.TopicSpec{{Name: topic, Partitions: 3, Retention: time.Hour}}, 3))

	newRelay := func(owner string) *Relay {
		return &Relay{DB: db, Streams: dynamo.NewStreamsClient(awsCfg, dc), Kafka: kc,
			Leases: dynamo.NewLeases(db, tables, 2*time.Second), Table: tables.Notifications, Topic: topic,
			Owner: owner, Log: slog.Default(), Poll: 50 * time.Millisecond, Discovery: 500 * time.Millisecond}
	}
	create := func(i int) {
		n := &domain.Notification{ID: fmt.Sprintf("n-%03d", i), Producer: "p", UserID: fmt.Sprintf("u%d", i%5),
			TemplateID: "t", Category: domain.CategoryTransactional, Priority: domain.PriorityHigh,
			CreatedAt: time.Now().Unix(), Status: domain.StatusAccepted}
		n.ComputeTTL()
		_, _, err := repo.Create(ctx, n, "")
		require.NoError(t, err)
	}

	for i := 0; i < 20; i++ {
		create(i)
	}
	stop := startRelay(t, newRelay("relay-a"))

	consumer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	t.Cleanup(consumer.Close)

	seen := map[string]int{}
	poll := func(want int) {
		deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		for len(seen) < want || deadline.Err() == nil && len(seen) < want {
			fetches := consumer.PollFetches(deadline)
			require.NoError(t, deadline.Err(), "timed out with %d/%d", len(seen), want)
			fetches.EachRecord(func(rec *kgo.Record) {
				var ev events.NotificationRequested
				require.NoError(t, proto.Unmarshal(rec.Value, &ev))
				require.Equal(t, ev.UserId, string(rec.Key))
				seen[ev.NotificationId]++
			})
		}
	}
	poll(20)

	stop()
	time.Sleep(3 * time.Second)
	create(20)
	startRelay(t, newRelay("relay-b"))
	poll(21)

	drain, cancelDrain := context.WithTimeout(ctx, 3*time.Second)
	defer cancelDrain()
	consumer.PollFetches(drain).EachRecord(func(rec *kgo.Record) {
		var ev events.NotificationRequested
		require.NoError(t, proto.Unmarshal(rec.Value, &ev))
		seen[ev.NotificationId]++
	})
	for id, c := range seen {
		require.Equal(t, 1, c, "duplicate publish for %s", id)
	}
}
