package dynamo

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
			AttrSK:       &types.AttributeValueMemberS{Value: fmt.Sprintf("%012d#%s", createdAt, notificationID)},
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
