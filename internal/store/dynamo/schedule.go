package dynamo

import (
	"context"
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
