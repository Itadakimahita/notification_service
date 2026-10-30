package dynamo

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"notification-service/internal/domain"
)

const AllChannels = "all"

type Statuses struct {
	db *dynamodb.Client
	t  Tables
}

// NewStatuses builds the delivery status repository.
func NewStatuses(db *dynamodb.Client, t Tables) *Statuses { return &Statuses{db: db, t: t} }

func (r *Statuses) key(nid string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{AttrPK: &types.AttributeValueMemberS{Value: nid}}
}

// Apply records a status only if it ranks above the stored one; stale events and unknown notifications are ignored.
func (r *Statuses) Apply(ctx context.Context, nid, channel string, st domain.Status, detail string) error {
	rank := &types.AttributeValueMemberN{Value: itoa(st.Rank())}
	in := &dynamodb.UpdateItemInput{TableName: aws.String(r.t.Notifications), Key: r.key(nid)}
	if channel == AllChannels {
		in.UpdateExpression = aws.String("SET #s = :st, status_rank = :r")
		in.ConditionExpression = aws.String("attribute_exists(pk) AND (attribute_not_exists(status_rank) OR status_rank < :r)")
		in.ExpressionAttributeNames = map[string]string{"#s": "status"}
		in.ExpressionAttributeValues = map[string]types.AttributeValue{":st": &types.AttributeValueMemberS{Value: string(st)}, ":r": rank}
	} else {
		in.UpdateExpression = aws.String("SET channel_status.#c = :st, channel_rank.#c = :r, channel_detail.#c = :d")
		in.ConditionExpression = aws.String("attribute_exists(pk) AND (attribute_not_exists(channel_rank.#c) OR channel_rank.#c < :r)")
		in.ExpressionAttributeNames = map[string]string{"#c": channel}
		in.ExpressionAttributeValues = map[string]types.AttributeValue{
			":st": &types.AttributeValueMemberS{Value: string(st)}, ":r": rank, ":d": &types.AttributeValueMemberS{Value: detail},
		}
	}

	_, err := r.db.UpdateItem(ctx, in)
	var cf *types.ConditionalCheckFailedException
	if errors.As(err, &cf) {
		return nil
	}
	if err != nil && channel != AllChannels && isMissingMap(err) {
		if ierr := r.initMaps(ctx, nid); ierr != nil {
			return ierr
		}
		_, err = r.db.UpdateItem(ctx, in)
		if errors.As(err, &cf) {
			return nil
		}
	}
	return err
}

func isMissingMap(err error) bool {
	var ae interface{ ErrorMessage() string }
	return errors.As(err, &ae) && strings.Contains(ae.ErrorMessage(), "document path")
}

// initMaps adds the per-channel maps to notifications created before they were part of the schema.
func (r *Statuses) initMaps(ctx context.Context, nid string) error {
	empty := &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}}
	_, err := r.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(r.t.Notifications),
		Key:                       r.key(nid),
		UpdateExpression:          aws.String("SET channel_status = if_not_exists(channel_status, :e), channel_rank = if_not_exists(channel_rank, :e), channel_detail = if_not_exists(channel_detail, :e)"),
		ConditionExpression:       aws.String("attribute_exists(pk)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":e": empty},
	})
	var cf *types.ConditionalCheckFailedException
	if errors.As(err, &cf) {
		return nil
	}
	return err
}

func itoa(n int) string { return strconv.Itoa(n) }
