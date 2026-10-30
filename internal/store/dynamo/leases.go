package dynamo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Lease struct {
	Checkpoint string
	Done       bool
}

type Leases struct {
	db  *dynamodb.Client
	t   Tables
	ttl time.Duration
}

// NewLeases builds a lease store where each lease expires after ttl unless renewed.
func NewLeases(db *dynamodb.Client, t Tables, ttl time.Duration) *Leases {
	return &Leases{db: db, t: t, ttl: ttl}
}

func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func n(v int64) types.AttributeValue  { return &types.AttributeValueMemberN{Value: fmt.Sprint(v)} }

func attrS(m map[string]types.AttributeValue, k string) string {
	if v, ok := m[k].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

// Acquire takes the lease if it is free, expired or already ours, returning its saved state.
func (l *Leases) Acquire(ctx context.Context, name, owner string) (Lease, bool, error) {
	now := time.Now()
	out, err := l.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(l.t.Leases),
		Key:                      map[string]types.AttributeValue{AttrPK: s(name)},
		UpdateExpression:         aws.String("SET #o = :me, expires_at = :exp, #t = :ttl"),
		ConditionExpression:      aws.String("attribute_not_exists(pk) OR #o = :me OR expires_at < :now"),
		ExpressionAttributeNames: map[string]string{"#o": "owner", "#t": AttrTTL},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":me": s(owner), ":now": n(now.Unix()), ":exp": n(now.Add(l.ttl).Unix()), ":ttl": n(now.Add(48 * time.Hour).Unix()),
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		var cf *types.ConditionalCheckFailedException
		if errors.As(err, &cf) {
			return Lease{}, false, nil
		}
		return Lease{}, false, err
	}
	done, _ := out.Attributes["done"].(*types.AttributeValueMemberBOOL)
	return Lease{Checkpoint: attrS(out.Attributes, "checkpoint"), Done: done != nil && done.Value}, true, nil
}

// Checkpoint renews our lease and stores progress; it returns false when the lease was lost.
func (l *Leases) Checkpoint(ctx context.Context, name, owner, checkpoint string, done bool) (bool, error) {
	now := time.Now()
	expr := "SET expires_at = :exp, #t = :ttl, done = :done"
	vals := map[string]types.AttributeValue{
		":me": s(owner), ":exp": n(now.Add(l.ttl).Unix()), ":ttl": n(now.Add(48 * time.Hour).Unix()),
		":done": &types.AttributeValueMemberBOOL{Value: done},
	}
	if checkpoint != "" {
		expr += ", checkpoint = :cp"
		vals[":cp"] = s(checkpoint)
	}
	_, err := l.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(l.t.Leases),
		Key:                       map[string]types.AttributeValue{AttrPK: s(name)},
		UpdateExpression:          aws.String(expr),
		ConditionExpression:       aws.String("#o = :me"),
		ExpressionAttributeNames:  map[string]string{"#o": "owner", "#t": AttrTTL},
		ExpressionAttributeValues: vals,
	})
	if err != nil {
		var cf *types.ConditionalCheckFailedException
		if errors.As(err, &cf) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// IsDone reports whether the named lease has been fully consumed.
func (l *Leases) IsDone(ctx context.Context, name string) (bool, error) {
	out, err := l.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(l.t.Leases),
		Key:            map[string]types.AttributeValue{AttrPK: s(name)},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil || out.Item == nil {
		return false, err
	}
	v, ok := out.Item["done"].(*types.AttributeValueMemberBOOL)
	return ok && v.Value, nil
}
