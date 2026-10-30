package dynamo

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const ScheduleShards = 16

type Schedule struct {
	db *dynamodb.Client
	t  Tables
}

// NewSchedule builds the delayed-delivery repository.
func NewSchedule(db *dynamodb.Client, t Tables) *Schedule { return &Schedule{db: db, t: t} }

// Bucket returns the partition key for a due minute and shard.
func Bucket(due time.Time, shard int) string {
	return fmt.Sprintf("%d#%02d", due.Unix()/60, shard)
}

// Put schedules a serialized request for later delivery.
func (r *Schedule) Put(ctx context.Context, notificationID, variant string, due time.Time, payload []byte) error {
	h := fnv.New32a()
	_, _ = h.Write([]byte(notificationID))
	shard := int(h.Sum32() % ScheduleShards)
	_, err := r.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.t.Schedule),
		Item: map[string]types.AttributeValue{
			AttrPK:    &types.AttributeValueMemberS{Value: Bucket(due, shard)},
			AttrSK:    &types.AttributeValueMemberS{Value: fmt.Sprintf("%012d#%s#%s", due.Unix(), notificationID, variant)},
			"payload": &types.AttributeValueMemberB{Value: payload},
			AttrTTL:   &types.AttributeValueMemberN{Value: fmt.Sprint(due.Add(7 * 24 * time.Hour).Unix())},
		},
	})
	return err
}

type ScheduledItem struct {
	PK      string
	SK      string
	Payload []byte
}

// Due returns up to limit items in a bucket whose due time is at or before nowUnix.
func (r *Schedule) Due(ctx context.Context, minute int64, shard int, nowUnix int64, limit int32) ([]ScheduledItem, error) {
	pk := fmt.Sprintf("%d#%02d", minute, shard)
	out, err := r.db.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.t.Schedule),
		KeyConditionExpression: aws.String("pk = :pk AND sk <= :max"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":  &types.AttributeValueMemberS{Value: pk},
			":max": &types.AttributeValueMemberS{Value: fmt.Sprintf("%012d~", nowUnix)},
		},
		ConsistentRead: aws.Bool(true),
		Limit:          aws.Int32(limit),
	})
	if err != nil {
		return nil, err
	}
	items := make([]ScheduledItem, 0, len(out.Items))
	for _, it := range out.Items {
		sk, _ := it[AttrSK].(*types.AttributeValueMemberS)
		payload, _ := it["payload"].(*types.AttributeValueMemberB)
		if sk == nil || payload == nil {
			continue
		}
		items = append(items, ScheduledItem{PK: pk, SK: sk.Value, Payload: payload.Value})
	}
	return items, nil
}

// Delete removes emitted items in batches of 25, retrying unprocessed ones.
func (r *Schedule) Delete(ctx context.Context, items []ScheduledItem) error {
	for start := 0; start < len(items); start += 25 {
		end := min(start+25, len(items))
		reqs := make([]types.WriteRequest, 0, end-start)
		for _, it := range items[start:end] {
			reqs = append(reqs, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{
				AttrPK: &types.AttributeValueMemberS{Value: it.PK},
				AttrSK: &types.AttributeValueMemberS{Value: it.SK},
			}}})
		}
		pending := map[string][]types.WriteRequest{r.t.Schedule: reqs}
		for attempt := 0; len(pending) > 0; attempt++ {
			if attempt >= 5 {
				return errors.New("schedule delete: unprocessed items remain")
			}
			out, err := r.db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: pending})
			if err != nil {
				return err
			}
			pending = out.UnprocessedItems
		}
	}
	return nil
}
