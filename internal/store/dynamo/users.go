package dynamo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"notification-service/internal/domain"
)

const (
	skProfile    = "PROFILE"
	prefPrefix   = "PREF#"
	devicePrefix = "DEVICE#"
)

type Users struct {
	db *dynamodb.Client
	t  Tables
}

// NewUsers builds the user repository.
func NewUsers(db *dynamodb.Client, t Tables) *Users { return &Users{db: db, t: t} }

// DeviceID derives a stable id from a device token.
func DeviceID(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:8])
}

func (r *Users) put(ctx context.Context, pk, sk string, v any) error {
	item, err := attributevalue.MarshalMap(v)
	if err != nil {
		return err
	}
	item[AttrPK] = &types.AttributeValueMemberS{Value: pk}
	item[AttrSK] = &types.AttributeValueMemberS{Value: sk}
	_, err = r.db.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(r.t.Users), Item: item})
	return err
}

// PutProfile creates or replaces the user profile.
func (r *Users) PutProfile(ctx context.Context, p domain.Profile) error {
	return r.put(ctx, p.UserID, skProfile, p)
}

// PutPreferences upserts the given preferences.
func (r *Users) PutPreferences(ctx context.Context, userID string, prefs []domain.Preference) error {
	for _, p := range prefs {
		if err := r.put(ctx, userID, prefPrefix+string(p.Category)+"#"+string(p.Channel), p); err != nil {
			return err
		}
	}
	return nil
}

// PutDevice registers a device and returns its id.
func (r *Users) PutDevice(ctx context.Context, userID string, d domain.Device) (string, error) {
	d.ID = DeviceID(d.Token)
	d.Active = true
	return d.ID, r.put(ctx, userID, devicePrefix+d.ID, d)
}

// DeleteDevice removes a device by id.
func (r *Users) DeleteDevice(ctx context.Context, userID, deviceID string) error {
	_, err := r.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(r.t.Users),
		Key: map[string]types.AttributeValue{
			AttrPK: &types.AttributeValueMemberS{Value: userID},
			AttrSK: &types.AttributeValueMemberS{Value: devicePrefix + deviceID},
		},
	})
	return err
}

// DeactivateDevice marks a device token as invalid after a provider rejection.
func (r *Users) DeactivateDevice(ctx context.Context, userID, deviceID string) error {
	_, err := r.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.t.Users),
		Key: map[string]types.AttributeValue{
			AttrPK: &types.AttributeValueMemberS{Value: userID},
			AttrSK: &types.AttributeValueMemberS{Value: devicePrefix + deviceID},
		},
		UpdateExpression:          aws.String("SET active = :f"),
		ConditionExpression:       aws.String("attribute_exists(pk)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":f": &types.AttributeValueMemberBOOL{Value: false}},
	})
	return err
}

// Load reads the profile, preferences and devices of a user with one query.
func (r *Users) Load(ctx context.Context, userID string) (domain.UserData, error) {
	expr, err := expression.NewBuilder().WithKeyCondition(expression.Key(AttrPK).Equal(expression.Value(userID))).Build()
	if err != nil {
		return domain.UserData{}, err
	}
	var data domain.UserData
	p := dynamodb.NewQueryPaginator(r.db, &dynamodb.QueryInput{
		TableName:                 aws.String(r.t.Users),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConsistentRead:            aws.Bool(true),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return domain.UserData{}, err
		}
		for _, item := range page.Items {
			sk, _ := item[AttrSK].(*types.AttributeValueMemberS)
			if sk == nil {
				continue
			}
			switch {
			case sk.Value == skProfile:
				data.Found = true
				err = attributevalue.UnmarshalMap(item, &data.Profile)
			case strings.HasPrefix(sk.Value, prefPrefix):
				var pref domain.Preference
				if err = attributevalue.UnmarshalMap(item, &pref); err == nil {
					data.Prefs = append(data.Prefs, pref)
				}
			case strings.HasPrefix(sk.Value, devicePrefix):
				var d domain.Device
				if err = attributevalue.UnmarshalMap(item, &d); err == nil {
					data.Devices = append(data.Devices, d)
				}
			}
			if err != nil {
				return domain.UserData{}, err
			}
		}
	}
	return data, nil
}
