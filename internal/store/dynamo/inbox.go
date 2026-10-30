package dynamo

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Inbox struct {
	db *dynamodb.Client
	t  Tables
}

// NewInbox builds the in-app inbox repository.
func NewInbox(db *dynamodb.Client, t Tables) *Inbox { return &Inbox{db: db, t: t} }

// Put stores an inbox item; the key derives from the message so replays overwrite instead of duplicating.
func (r *Inbox) Put(ctx context.Context, userID, notificationID, title, body string, createdAt int64) error {
	_, err := r.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.t.Inbox),
		Item: map[string]types.AttributeValue{
			AttrPK:       &types.AttributeValueMemberS{Value: userID},
			AttrSK:       &types.AttributeValueMemberS{Value: fmt.Sprintf("%012d_%s", createdAt, notificationID)},
			"title":      &types.AttributeValueMemberS{Value: title},
			"body":       &types.AttributeValueMemberS{Value: body},
			"read":       &types.AttributeValueMemberBOOL{Value: false},
			AttrTTL:      &types.AttributeValueMemberN{Value: fmt.Sprint(time.Now().Add(RetentionTTL).Unix())},
			"created_at": &types.AttributeValueMemberN{Value: fmt.Sprint(createdAt)},
		},
	})
	return err
}

const RetentionTTL = 30 * 24 * time.Hour

type InboxItem struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Body      string `json:"body"`
	Read      bool   `json:"read"`
	CreatedAt int64  `json:"created_at"`
}

// List returns a user's inbox newest first with an opaque cursor for the next page.
func (r *Inbox) List(ctx context.Context, userID string, limit int32, cursor string) ([]InboxItem, string, error) {
	in := &dynamodb.QueryInput{
		TableName:                 aws.String(r.t.Inbox),
		KeyConditionExpression:    aws.String("pk = :p"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":p": &types.AttributeValueMemberS{Value: userID}},
		ScanIndexForward:          aws.Bool(false),
		Limit:                     aws.Int32(limit),
	}
	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, "", errors.New("invalid cursor")
		}
		in.ExclusiveStartKey = map[string]types.AttributeValue{
			AttrPK: &types.AttributeValueMemberS{Value: userID},
			AttrSK: &types.AttributeValueMemberS{Value: string(raw)},
		}
	}
	out, err := r.db.Query(ctx, in)
	if err != nil {
		return nil, "", err
	}
	items := make([]InboxItem, 0, len(out.Items))
	for _, it := range out.Items {
		var rec struct {
			SK        string `dynamodbav:"sk"`
			Title     string `dynamodbav:"title"`
			Body      string `dynamodbav:"body"`
			Read      bool   `dynamodbav:"read"`
			CreatedAt int64  `dynamodbav:"created_at"`
		}
		if err := attributevalue.UnmarshalMap(it, &rec); err != nil {
			return nil, "", err
		}
		items = append(items, InboxItem{ID: rec.SK, Title: rec.Title, Body: rec.Body, Read: rec.Read, CreatedAt: rec.CreatedAt})
	}
	next := ""
	if k, ok := out.LastEvaluatedKey[AttrSK].(*types.AttributeValueMemberS); ok {
		next = base64.RawURLEncoding.EncodeToString([]byte(k.Value))
	}
	return items, next, nil
}

// MarkRead flags one inbox item as read; it returns ErrNotFound for unknown items.
func (r *Inbox) MarkRead(ctx context.Context, userID, itemID string) error {
	_, err := r.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.t.Inbox),
		Key: map[string]types.AttributeValue{
			AttrPK: &types.AttributeValueMemberS{Value: userID},
			AttrSK: &types.AttributeValueMemberS{Value: itemID},
		},
		UpdateExpression:          aws.String("SET #r = :t"),
		ConditionExpression:       aws.String("attribute_exists(pk)"),
		ExpressionAttributeNames:  map[string]string{"#r": "read"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":t": &types.AttributeValueMemberBOOL{Value: true}},
	})
	var cf *types.ConditionalCheckFailedException
	if errors.As(err, &cf) {
		return ErrNotFound
	}
	return err
}
