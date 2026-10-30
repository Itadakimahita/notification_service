package worker

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	pb "notification-service/internal/events/notification/v1"
	"notification-service/internal/kafkax"
)

const dueHeader = "due_at_ms"

type Consumer struct {
	Worker      *Worker
	Client      *kgo.Client
	Log         *slog.Logger
	Parallelism int
	BatchSize   int
	Waits       bool
}

// Run consumes a lane or retry tier and processes messages in parallel batches.
func (c *Consumer) Run(ctx context.Context) error {
	return kafkax.ProcessBatches(ctx, c.Client, c.BatchSize, c.Parallelism, c.handle, c.Log)
}

// handle decodes a record, honors retry delays, and converts the outcome into follow-up records.
func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) []*kgo.Record {
	var msg pb.DispatchMessage
	if err := proto.Unmarshal(rec.Value, &msg); err != nil {
		c.Log.Error("undecodable dispatch message", "topic", rec.Topic, "offset", rec.Offset, "err", err)
		return []*kgo.Record{{Topic: kafkax.DLQTopic(c.Worker.Channel), Key: rec.Key, Value: rec.Value,
			Headers: []kgo.RecordHeader{{Key: "error", Value: []byte(err.Error())}}}}
	}
	if c.Waits && !waitUntilDue(ctx, rec) {
		return nil
	}

	var res Result
	for backoff := 100 * time.Millisecond; ; backoff = min(backoff*2, 5*time.Second) {
		var err error
		if res, err = c.Worker.Process(ctx, &msg); err == nil {
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		c.Log.Warn("processing failed, retrying", "notification_id", msg.NotificationId, "err", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
	}
	return c.records(&res)
}

func (c *Consumer) records(res *Result) []*kgo.Record {
	var out []*kgo.Record
	ch := c.Worker.Channel
	if r := res.Retry; r != nil {
		val, _ := proto.Marshal(r.Msg)
		out = append(out, &kgo.Record{
			Topic:   kafkax.RetryTopic(ch, r.Delay),
			Key:     []byte(r.Msg.UserId),
			Value:   val,
			Headers: []kgo.RecordHeader{{Key: dueHeader, Value: []byte(strconv.FormatInt(c.Worker.Now().Add(r.Delay).UnixMilli(), 10))}},
		})
	}
	if d := res.Dead; d != nil {
		val, _ := proto.Marshal(d.Msg)
		out = append(out, &kgo.Record{
			Topic:   kafkax.DLQTopic(ch),
			Key:     []byte(d.Msg.UserId),
			Value:   val,
			Headers: []kgo.RecordHeader{{Key: "error", Value: []byte(d.Reason)}},
		})
	}
	for _, s := range res.Status {
		val, _ := proto.Marshal(s)
		out = append(out, &kgo.Record{Topic: kafkax.TopicStatus, Key: []byte(s.NotificationId), Value: val})
	}
	return out
}

// waitUntilDue holds a retry record until its due time; it returns false when ctx ends first.
func waitUntilDue(ctx context.Context, rec *kgo.Record) bool {
	for _, h := range rec.Headers {
		if h.Key != dueHeader {
			continue
		}
		ms, err := strconv.ParseInt(string(h.Value), 10, 64)
		if err != nil {
			return true
		}
		wait := time.Until(time.UnixMilli(ms))
		if wait <= 0 {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(wait):
		}
	}
	return true
}
