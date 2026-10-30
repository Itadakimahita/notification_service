package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"

	"notification-service/internal/config"
)

// LoadAWS builds an AWS config, using static local credentials when a custom endpoint is set.
func LoadAWS(ctx context.Context, c config.Dynamo) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(c.Region)}
	if c.Endpoint != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")))
	}
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}

// NewClient returns a DynamoDB client honoring a local endpoint override.
func NewClient(awsCfg aws.Config, c config.Dynamo) *dynamodb.Client {
	return dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	})
}

// NewStreamsClient returns a DynamoDB Streams client honoring a local endpoint override.
func NewStreamsClient(awsCfg aws.Config, c config.Dynamo) *dynamodbstreams.Client {
	return dynamodbstreams.NewFromConfig(awsCfg, func(o *dynamodbstreams.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	})
}
