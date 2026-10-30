package dynamo

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"notification-service/internal/domain"
)

type Templates struct {
	db *dynamodb.Client
	t  Tables
}

// NewTemplates builds the template repository.
func NewTemplates(db *dynamodb.Client, t Tables) *Templates { return &Templates{db: db, t: t} }

func templateSKPrefix(ch domain.Channel, locale string) string {
	return fmt.Sprintf("%s#%s#", ch, locale)
}

// GetLatest returns the highest version for a template, channel and exact locale.
func (r *Templates) GetLatest(ctx context.Context, id string, ch domain.Channel, locale string) (*domain.Template, error) {
	out, err := r.db.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(r.t.Templates),
		KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :p)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: id},
			":p":  &types.AttributeValueMemberS{Value: templateSKPrefix(ch, locale)},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(1),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Items) == 0 {
		return nil, ErrNotFound
	}
	var t domain.Template
	if err := attributevalue.UnmarshalMap(out.Items[0], &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// Put stores a new version of a template and returns it.
func (r *Templates) Put(ctx context.Context, t domain.Template) (domain.Template, error) {
	for range 5 {
		next := 1
		if cur, err := r.GetLatest(ctx, t.ID, t.Channel, t.Locale); err == nil {
			next = cur.Version + 1
		} else if !errors.Is(err, ErrNotFound) {
			return t, err
		}
		t.Version = next
		item, err := attributevalue.MarshalMap(t)
		if err != nil {
			return t, err
		}
		item[AttrPK] = &types.AttributeValueMemberS{Value: t.ID}
		item[AttrSK] = &types.AttributeValueMemberS{Value: fmt.Sprintf("%s%06d", templateSKPrefix(t.Channel, t.Locale), next)}
		_, err = r.db.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:           aws.String(r.t.Templates),
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(sk)"),
		})
		var cf *types.ConditionalCheckFailedException
		if errors.As(err, &cf) {
			continue
		}
		return t, err
	}
	return t, errors.New("template version conflict")
}
