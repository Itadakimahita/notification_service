package blob

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/require"

	"notification-service/internal/blob/blobtest"
)

func newStore(t *testing.T) (*Store, *blobtest.FakeS3) {
	t.Helper()
	fake := blobtest.NewFakeS3()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("k", "s", "")}
	return New(cfg, srv.URL, "bkt"), fake
}

func TestPresignedUploadThenRangedRead(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	require.NoError(t, s.EnsureBucket(ctx))

	url, err := s.PresignPut(ctx, "campaigns/c1/audience.csv", time.Hour)
	require.NoError(t, err)
	req, _ := http.NewRequest(http.MethodPut, url, strings.NewReader("user_id\nu1\nu2\nu3\n"))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	full, err := s.Get(ctx, "campaigns/c1/audience.csv", 0)
	require.NoError(t, err)
	b, _ := io.ReadAll(full)
	full.Close()
	require.Equal(t, "user_id\nu1\nu2\nu3\n", string(b))

	part, err := s.Get(ctx, "campaigns/c1/audience.csv", 11)
	require.NoError(t, err)
	b, _ = io.ReadAll(part)
	part.Close()
	require.Equal(t, "u2\nu3\n", string(b), "offset must resume mid-file")
}

func TestMissingObjectMapsToErrNotFound(t *testing.T) {
	s, _ := newStore(t)
	_, err := s.Get(context.Background(), "nope", 0)
	require.ErrorIs(t, err, ErrNotFound)
}
