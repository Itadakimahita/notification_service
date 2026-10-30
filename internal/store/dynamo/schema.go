package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	AttrPK  = "pk"
	AttrSK  = "sk"
	AttrTTL = "ttl"
)

type Tables struct {
	Notifications string
	Idempotency   string
	Users         string
	Templates     string
	Schedule      string
	Inbox         string
	Leases        string
	Campaigns     string
}

// NewTables derives all table names from a shared prefix.
func NewTables(prefix string) Tables {
	return Tables{
		Notifications: prefix + "notifications",
		Idempotency:   prefix + "idempotency",
		Users:         prefix + "users",
		Templates:     prefix + "templates",
		Schedule:      prefix + "schedule",
		Inbox:         prefix + "inbox",
		Leases:        prefix + "leases",
		Campaigns:     prefix + "campaigns",
	}
}

type tableSpec struct {
	name   string
	sorted bool
	stream bool
	ttl    bool
}

func (t Tables) specs() []tableSpec {
	return []tableSpec{
		{name: t.Notifications, stream: true, ttl: true},
		{name: t.Idempotency, ttl: true},
		{name: t.Users, sorted: true},
		{name: t.Templates, sorted: true},
		{name: t.Schedule, sorted: true},
		{name: t.Inbox, sorted: true, ttl: true},
		{name: t.Leases},
		{name: t.Campaigns},
	}
}

// EnsureTables creates missing tables, waits until active and enables TTL.
func EnsureTables(ctx context.Context, db *dynamodb.Client, t Tables) error {
	for _, s := range t.specs() {
		if err := ensureTable(ctx, db, s); err != nil {
			return fmt.Errorf("table %s: %w", s.name, err)
		}
	}
	return nil
}

func ensureTable(ctx context.Context, db *dynamodb.Client, s tableSpec) error {
	in := &dynamodb.CreateTableInput{
		TableName:   aws.String(s.name),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(AttrPK), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(AttrPK), KeyType: types.KeyTypeHash},
		},
	}
	if s.sorted {
		in.AttributeDefinitions = append(in.AttributeDefinitions,
			types.AttributeDefinition{AttributeName: aws.String(AttrSK), AttributeType: types.ScalarAttributeTypeS})
		in.KeySchema = append(in.KeySchema,
			types.KeySchemaElement{AttributeName: aws.String(AttrSK), KeyType: types.KeyTypeRange})
	}
	if s.stream {
		in.StreamSpecification = &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewImage,
		}
	}

	_, err := db.CreateTable(ctx, in)
	var inUse *types.ResourceInUseException
	if err != nil && !errors.As(err, &inUse) {
		return err
	}

	waiter := dynamodb.NewTableExistsWaiter(db)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(s.name)}, time.Minute); err != nil {
		return err
	}

	if s.ttl {
		_, err := db.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
			TableName: aws.String(s.name),
			TimeToLiveSpecification: &types.TimeToLiveSpecification{
				AttributeName: aws.String(AttrTTL),
				Enabled:       aws.Bool(true),
			},
		})
		if err != nil && !isTTLAlreadyEnabled(err) {
			return fmt.Errorf("enable ttl: %w", err)
		}
	}
	return nil
}

func isTTLAlreadyEnabled(err error) bool {
	var ve interface{ ErrorMessage() string }
	return errors.As(err, &ve) && strings.Contains(ve.ErrorMessage(), "already enabled")
}
