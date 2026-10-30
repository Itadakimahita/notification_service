package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var ErrNotFound = errors.New("object not found")

type Store struct {
	cli     *s3.Client
	presign *s3.PresignClient
	bucket  string
}

// New builds an S3 client that works against AWS S3 or any S3-compatible endpoint.
func New(awsCfg aws.Config, endpoint, bucket string) *Store {
	cli := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	return &Store{cli: cli, presign: s3.NewPresignClient(cli), bucket: bucket}
}

// PresignPut returns a URL that lets a producer upload an object directly.
func (s *Store) PresignPut(ctx context.Context, key string, ttl time.Duration) (string, error) {
	r, err := s.presign.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", err
	}
	return r.URL, nil
}

// Get streams an object starting at a byte offset.
func (s *Store) Get(ctx context.Context, key string, offset int64) (io.ReadCloser, error) {
	in := &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}
	if offset > 0 {
		in.Range = aws.String(fmt.Sprintf("bytes=%d-", offset))
	}
	out, err := s.cli.GetObject(ctx, in)
	if err != nil {
		var nf *types.NoSuchKey
		if errors.As(err, &nf) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return out.Body, nil
}

// Put stores a small object.
func (s *Store) Put(ctx context.Context, key string, body io.Reader) error {
	_, err := s.cli.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key), Body: body})
	return err
}

// EnsureBucket creates the bucket if it does not exist yet.
func (s *Store) EnsureBucket(ctx context.Context) error {
	_, err := s.cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(s.bucket)})
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return nil
	}
	return err
}
