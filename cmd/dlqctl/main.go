package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"notification-service/internal/domain"
	pb "notification-service/internal/events/notification/v1"
	"notification-service/internal/kafkax"
)

// main inspects or replays a channel's dead-letter topic.
func main() {
	if len(os.Args) < 2 || (os.Args[1] != "inspect" && os.Args[1] != "replay") {
		fmt.Fprintln(os.Stderr, "usage: dlqctl <inspect|replay> --channel <push|email|sms|inapp> [--limit N]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	channel := fs.String("channel", "", "channel whose DLQ to read")
	limit := fs.Int("limit", 100, "maximum messages to process")
	brokers := fs.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092,localhost:9094,localhost:9096"), "kafka brokers")
	_ = fs.Parse(os.Args[2:])
	ch := domain.Channel(*channel)
	if !ch.Valid() {
		fmt.Fprintln(os.Stderr, "--channel must be push, email, sms or inapp")
		os.Exit(2)
	}

	var err error
	if cmd == "inspect" {
		err = inspect(strings.Split(*brokers, ","), ch, *limit)
	} else {
		err = replay(strings.Split(*brokers, ","), ch, *limit)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func inspect(brokers []string, ch domain.Channel, limit int) error {
	topic := kafkax.DLQTopic(ch)
	cl, err := kafkax.NewClient(brokers, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return err
	}
	defer cl.Close()
	ctx := context.Background()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil {
		return err
	}
	remaining := int64(0)
	ends.Each(func(o kadm.ListedOffset) { remaining += o.Offset })
	shown := 0
	for remaining > 0 && shown < limit {
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if pollCtx.Err() != nil && len(fetches.Records()) == 0 {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			remaining--
			if shown >= limit {
				return
			}
			shown++
			var m pb.DispatchMessage
			if proto.Unmarshal(r.Value, &m) != nil {
				fmt.Printf("p%d/o%d undecodable\n", r.Partition, r.Offset)
				return
			}
			reason := ""
			for _, h := range r.Headers {
				if h.Key == "error" {
					reason = string(h.Value)
				}
			}
			fmt.Printf("p%d/o%d notification=%s user=%s attempt=%d error=%q\n", r.Partition, r.Offset, m.NotificationId, m.UserId, m.Attempt, reason)
		})
	}
	fmt.Printf("%d message(s) shown\n", shown)
	return nil
}

func replay(brokers []string, ch domain.Channel, limit int) error {
	cl, err := kafkax.NewClient(brokers,
		kgo.ConsumerGroup("dlqctl-"+string(ch)),
		kgo.ConsumeTopics(kafkax.DLQTopic(ch)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	ctx := context.Background()
	replayed := 0
	for replayed < limit {
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		fetches := cl.PollRecords(pollCtx, min(500, limit-replayed))
		cancel()
		recs := fetches.Records()
		if len(recs) == 0 {
			break
		}
		var out []*kgo.Record
		for _, r := range recs {
			var m pb.DispatchMessage
			if err := proto.Unmarshal(r.Value, &m); err != nil {
				continue
			}
			m.Attempt = 0
			val, _ := proto.Marshal(&m)
			out = append(out, &kgo.Record{
				Topic: kafkax.DispatchTopic(ch, domain.Priority(m.Priority)),
				Key:   []byte(m.UserId),
				Value: val,
			})
		}
		if err := cl.ProduceSync(ctx, out...).FirstErr(); err != nil {
			return err
		}
		if err := cl.CommitRecords(ctx, recs...); err != nil {
			return err
		}
		replayed += len(out)
	}
	fmt.Printf("%d message(s) replayed to dispatch.%s\n", replayed, ch)
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
