package dynamo

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type CampaignStatus string

const (
	CampaignDraft     CampaignStatus = "DRAFT"
	CampaignRunning   CampaignStatus = "RUNNING"
	CampaignPaused    CampaignStatus = "PAUSED"
	CampaignCompleted CampaignStatus = "COMPLETED"
	CampaignCancelled CampaignStatus = "CANCELLED"
	CampaignFailed    CampaignStatus = "FAILED"
)

var ErrBadTransition = errors.New("campaign status transition not allowed")

type Campaign struct {
	ID          string            `dynamodbav:"pk" json:"campaign_id"`
	Name        string            `dynamodbav:"name" json:"name"`
	TemplateID  string            `dynamodbav:"template_id" json:"template_id"`
	Category    string            `dynamodbav:"category" json:"category"`
	Channels    []string          `dynamodbav:"channels,omitempty" json:"channels,omitempty"`
	Data        map[string]string `dynamodbav:"data,omitempty" json:"data,omitempty"`
	RatePerSec  int               `dynamodbav:"rate_per_sec" json:"rate_per_sec"`
	AudienceKey string            `dynamodbav:"audience_key" json:"audience_key"`
	Status      CampaignStatus    `dynamodbav:"status" json:"status"`
	Offset      int64             `dynamodbav:"offset" json:"-"`
	Emitted     int64             `dynamodbav:"emitted" json:"emitted"`
	Skipped     int64             `dynamodbav:"skipped" json:"skipped"`
	Error       string            `dynamodbav:"error,omitempty" json:"error,omitempty"`
	CreatedAt   int64             `dynamodbav:"created_at" json:"created_at"`
}

type Campaigns struct {
	db *dynamodb.Client
	t  Tables
}

// NewCampaigns builds the campaign repository.
func NewCampaigns(db *dynamodb.Client, t Tables) *Campaigns { return &Campaigns{db: db, t: t} }

func (r *Campaigns) key(id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{AttrPK: &types.AttributeValueMemberS{Value: id}}
}

// Create stores a new campaign.
func (r *Campaigns) Create(ctx context.Context, c *Campaign) error {
	c.CreatedAt = time.Now().Unix()
	item, err := attributevalue.MarshalMap(c)
	if err != nil {
		return err
	}
	_, err = r.db.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(r.t.Campaigns), Item: item, ConditionExpression: aws.String("attribute_not_exists(pk)")})
	return err
}

// Get loads a campaign.
func (r *Campaigns) Get(ctx context.Context, id string) (*Campaign, error) {
	out, err := r.db.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(r.t.Campaigns), Key: r.key(id), ConsistentRead: aws.Bool(true)})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, ErrNotFound
	}
	var c Campaign
	return &c, attributevalue.UnmarshalMap(out.Item, &c)
}

// SetStatus moves a campaign to a new status only from one of the allowed current statuses.
func (r *Campaigns) SetStatus(ctx context.Context, id string, from []CampaignStatus, to CampaignStatus, errMsg string) error {
	names := map[string]string{"#s": "status"}
	vals := map[string]types.AttributeValue{":to": &types.AttributeValueMemberS{Value: string(to)}}
	var in []string
	for i, f := range from {
		k := ":f" + string(rune('0'+i))
		vals[k] = &types.AttributeValueMemberS{Value: string(f)}
		in = append(in, k)
	}
	expr := "SET #s = :to"
	if errMsg != "" {
		expr += ", #e = :err"
		names["#e"] = "error"
		vals[":err"] = &types.AttributeValueMemberS{Value: errMsg}
	}
	_, err := r.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.t.Campaigns), Key: r.key(id), UpdateExpression: aws.String(expr),
		ConditionExpression:       aws.String("#s IN (" + strings.Join(in, ",") + ")"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: vals,
	})
	var cf *types.ConditionalCheckFailedException
	if errors.As(err, &cf) {
		return ErrBadTransition
	}
	return err
}

// Progress stores the resume offset and counters while the campaign is still running.
func (r *Campaigns) Progress(ctx context.Context, id string, offset, emitted, skipped int64) error {
	_, err := r.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(r.t.Campaigns), Key: r.key(id),
		UpdateExpression:         aws.String("SET #o = :o, emitted = :e, skipped = :s"),
		ExpressionAttributeNames: map[string]string{"#o": "offset"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":o": &types.AttributeValueMemberN{Value: itoa(int(offset))},
			":e": &types.AttributeValueMemberN{Value: itoa(int(emitted))},
			":s": &types.AttributeValueMemberN{Value: itoa(int(skipped))},
		},
	})
	return err
}

// ListRunning returns campaigns currently in the RUNNING state.
func (r *Campaigns) ListRunning(ctx context.Context) ([]Campaign, error) {
	var out []Campaign
	p := dynamodb.NewScanPaginator(r.db, &dynamodb.ScanInput{
		TableName:                 aws.String(r.t.Campaigns),
		FilterExpression:          aws.String("#s = :r"),
		ExpressionAttributeNames:  map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":r": &types.AttributeValueMemberS{Value: string(CampaignRunning)}},
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		var batch []Campaign
		if err := attributevalue.UnmarshalListOfMaps(page.Items, &batch); err != nil {
			return nil, err
		}
		out = append(out, batch...)
	}
	return out, nil
}
