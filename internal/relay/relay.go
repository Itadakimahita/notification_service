package relay

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dsattr "github.com/aws/aws-sdk-go-v2/feature/dynamodbstreams/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	sttypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"notification-service/internal/domain"
	events "notification-service/internal/events/notification/v1"
	"notification-service/internal/store/dynamo"
)

var (
	published  = promauto.NewCounter(prometheus.CounterOpts{Name: "relay_published_total", Help: "Events published to Kafka."})
	ownedGauge = promauto.NewGauge(prometheus.GaugeOpts{Name: "relay_owned_shards", Help: "Stream shards currently leased."})
)

type Relay struct {
	DB        *dynamodb.Client
	Streams   *dynamodbstreams.Client
	Kafka     *kgo.Client
	Leases    *dynamo.Leases
	Table     string
	Topic     string
	Owner     string
	Log       *slog.Logger
	Poll      time.Duration
	Discovery time.Duration

	mu    sync.Mutex
	owned map[string]bool
}

// Run discovers stream shards, leases them and forwards inserts to Kafka until ctx ends.
func (r *Relay) Run(ctx context.Context) error {
	r.owned = map[string]bool{}
	var wg sync.WaitGroup
	defer wg.Wait()

	tick := time.NewTicker(r.Discovery)
	defer tick.Stop()
	for {
		if err := r.discover(ctx, &wg); err != nil && ctx.Err() == nil {
			r.Log.Error("shard discovery failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func (r *Relay) discover(ctx context.Context, wg *sync.WaitGroup) error {
	desc, err := r.DB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(r.Table)})
	if err != nil {
		return err
	}
	arn := desc.Table.LatestStreamArn
	var shards []sttypes.Shard
	var start *string
	for {
		out, err := r.Streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: arn, ExclusiveStartShardId: start})
		if err != nil {
			return err
		}
		shards = append(shards, out.StreamDescription.Shards...)
		if out.StreamDescription.LastEvaluatedShardId == nil {
			break
		}
		start = out.StreamDescription.LastEvaluatedShardId
	}

	present := map[string]bool{}
	for _, sh := range shards {
		present[aws.ToString(sh.ShardId)] = true
	}
	for _, sh := range shards {
		id := aws.ToString(sh.ShardId)
		if r.isOwned(id) {
			continue
		}
		if parent := aws.ToString(sh.ParentShardId); parent != "" && present[parent] {
			if done, err := r.Leases.IsDone(ctx, leaseName(parent)); err != nil || !done {
				continue
			}
		}
		lease, ok, err := r.Leases.Acquire(ctx, leaseName(id), r.Owner)
		if err != nil || !ok || lease.Done {
			continue
		}
		r.setOwned(id, true)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer r.setOwned(id, false)
			if err := r.consume(ctx, arn, id, lease.Checkpoint); err != nil && ctx.Err() == nil {
				r.Log.Error("shard consumer stopped", "shard", id, "err", err)
			}
		}()
	}
	return nil
}

func leaseName(shard string) string { return "relay#" + shard }

func (r *Relay) isOwned(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.owned[id]
}

func (r *Relay) setOwned(id string, v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v {
		r.owned[id] = true
	} else {
		delete(r.owned, id)
	}
	ownedGauge.Set(float64(len(r.owned)))
}

// consume reads one shard, publishes inserts, and checkpoints after each successful batch.
func (r *Relay) consume(ctx context.Context, arn *string, shard, checkpoint string) error {
	in := &dynamodbstreams.GetShardIteratorInput{StreamArn: arn, ShardId: aws.String(shard)}
	if checkpoint != "" {
		in.ShardIteratorType = sttypes.ShardIteratorTypeAfterSequenceNumber
		in.SequenceNumber = aws.String(checkpoint)
	} else {
		in.ShardIteratorType = sttypes.ShardIteratorTypeTrimHorizon
	}
	itOut, err := r.Streams.GetShardIterator(ctx, in)
	if err != nil {
		return err
	}
	it := itOut.ShardIterator
	name := leaseName(shard)

	for ctx.Err() == nil {
		out, err := r.Streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: it, Limit: aws.Int32(1000)})
		if err != nil {
			return err
		}
		if len(out.Records) > 0 {
			recs, err := toKafka(out.Records, r.Topic)
			if err != nil {
				return err
			}
			if len(recs) > 0 {
				if err := r.Kafka.ProduceSync(ctx, recs...).FirstErr(); err != nil {
					return err
				}
				published.Add(float64(len(recs)))
			}
		}

		last := ""
		if n := len(out.Records); n > 0 {
			last = aws.ToString(out.Records[n-1].Dynamodb.SequenceNumber)
		}
		closed := out.NextShardIterator == nil
		cpCtx, cancelCp := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		held, err := r.Leases.Checkpoint(cpCtx, name, r.Owner, last, closed)
		cancelCp()
		if err != nil {
			return err
		}
		if !held {
			r.Log.Warn("lease lost", "shard", shard)
			return nil
		}
		if closed {
			return nil
		}
		it = out.NextShardIterator
		if len(out.Records) == 0 {
			select {
			case <-ctx.Done():
			case <-time.After(r.Poll):
			}
		}
	}
	return nil
}

func toKafka(records []sttypes.Record, topic string) ([]*kgo.Record, error) {
	var out []*kgo.Record
	for _, rec := range records {
		if rec.EventName != sttypes.OperationTypeInsert || rec.Dynamodb == nil {
			continue
		}
		var n domain.Notification
		if err := dsattr.UnmarshalMap(rec.Dynamodb.NewImage, &n); err != nil {
			return nil, err
		}
		val, err := proto.Marshal(requestedEvent(&n))
		if err != nil {
			return nil, err
		}
		rec := &kgo.Record{
			Topic:   topic,
			Key:     []byte(n.UserID),
			Value:   val,
			Headers: []kgo.RecordHeader{{Key: "notification_id", Value: []byte(n.ID)}},
		}
		if n.TraceParent != "" {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: "traceparent", Value: []byte(n.TraceParent)})
		}
		out = append(out, rec)
	}
	return out, nil
}

func requestedEvent(n *domain.Notification) *events.NotificationRequested {
	channels := make([]string, len(n.Channels))
	for i, c := range n.Channels {
		channels[i] = string(c)
	}
	return &events.NotificationRequested{
		NotificationId: n.ID, Producer: n.Producer, UserId: n.UserID, TemplateId: n.TemplateID,
		Category: string(n.Category), Priority: string(n.Priority), Channels: channels, Data: n.Data,
		SendAt: n.SendAt, ExpiresAt: n.ExpiresAt, CreatedAt: n.CreatedAt,
	}
}
