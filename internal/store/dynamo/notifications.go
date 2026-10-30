package dynamo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"notification-service/internal/domain"
)

const idempotencyTTL = 24 * time.Hour

var ErrNotFound = errors.New("not found")

type Notifications struct {
	db *dynamodb.Client
	t  Tables
}

// NewNotifications builds the notification repository.
func NewNotifications(db *dynamodb.Client, t Tables) *Notifications {
	return &Notifications{db: db, t: t}
}

// Create atomically stores the notification and its idempotency key; it returns the existing id on replay.
func (r *Notifications) Create(ctx context.Context, n *domain.Notification, idemKey string) (string, bool, error) {
	item, err := attributevalue.MarshalMap(n)
	if err != nil {
		return "", false, err
	}
	notifPut := &types.Put{
		TableName:           aws.String(r.t.Notifications),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	}
	if idemKey == "" {
		_, err := r.db.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:           notifPut.TableName,
			Item:                notifPut.Item,
			ConditionExpression: notifPut.ConditionExpression,
		})
		return n.ID, err == nil, err
	}

	idemPK := n.Producer + "#" + idemKey
	_, err = r.db.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{Put: &types.Put{
				TableName: aws.String(r.t.Idempotency),
				Item: map[string]types.AttributeValue{
					AttrPK:            &types.AttributeValueMemberS{Value: idemPK},
					"notification_id": &types.AttributeValueMemberS{Value: n.ID},
					AttrTTL:           &types.AttributeValueMemberN{Value: fmt.Sprint(time.Now().Add(idempotencyTTL).Unix())},
				},
				ConditionExpression: aws.String("attribute_not_exists(pk)"),
			}},
			{Put: notifPut},
		},
	})
	if err == nil {
		return n.ID, true, nil
	}

	var tce *types.TransactionCanceledException
	if errors.As(err, &tce) && len(tce.CancellationReasons) > 0 &&
		aws.ToString(tce.CancellationReasons[0].Code) == "ConditionalCheckFailed" {
		id, gerr := r.idempotentID(ctx, idemPK)
		return id, false, gerr
	}
	return "", false, err
}

func (r *Notifications) idempotentID(ctx context.Context, pk string) (string, error) {
	out, err := r.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.t.Idempotency),
		Key:            map[string]types.AttributeValue{AttrPK: &types.AttributeValueMemberS{Value: pk}},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return "", err
	}
	v, ok := out.Item["notification_id"].(*types.AttributeValueMemberS)
	if !ok {
		return "", ErrNotFound
	}
	return v.Value, nil
}

// Get loads a notification by id.
func (r *Notifications) Get(ctx context.Context, id string) (*domain.Notification, error) {
	out, err := r.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(r.t.Notifications),
		Key:       map[string]types.AttributeValue{AttrPK: &types.AttributeValueMemberS{Value: id}},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, ErrNotFound
	}
	var n domain.Notification
	if err := attributevalue.UnmarshalMap(out.Item, &n); err != nil {
		return nil, err
	}
	return &n, nil
}

// Ping verifies the notifications table is reachable.
func (r *Notifications) Ping(ctx context.Context) error {
	_, err := r.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(r.t.Notifications)})
	return err
}
