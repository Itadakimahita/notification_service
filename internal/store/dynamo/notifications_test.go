package dynamo

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"notification-service/internal/config"
	"notification-service/internal/domain"
)

func newTestRepo(t *testing.T) *Notifications {
	t.Helper()
	endpoint := os.Getenv("DYNAMO_ENDPOINT")
	if endpoint == "" {
		t.Skip("DYNAMO_ENDPOINT not set")
	}
	c := config.Dynamo{Endpoint: endpoint, Region: "us-east-1", TablePrefix: fmt.Sprintf("t%d_", time.Now().UnixNano())}
	ctx := context.Background()
	awsCfg, err := LoadAWS(ctx, c)
	require.NoError(t, err)
	db := NewClient(awsCfg, c)
	tables := NewTables(c.TablePrefix)
	require.NoError(t, EnsureTables(ctx, db, tables))
	return NewNotifications(db, tables)
}

func sample(id string) *domain.Notification {
	n := &domain.Notification{ID: id, Producer: "p", UserID: "u", TemplateID: "t",
		Category: domain.CategoryTransactional, Priority: domain.PriorityHigh,
		CreatedAt: time.Now().Unix(), Status: domain.StatusAccepted, Data: map[string]string{"a": "b"}}
	n.ComputeTTL()
	return n
}

func TestCreateIsIdempotent(t *testing.T) {
	r := newTestRepo(t)
	ctx := context.Background()

	id, created, err := r.Create(ctx, sample("n1"), "key")
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "n1", id)

	id, created, err = r.Create(ctx, sample("n2"), "key")
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, "n1", id)

	_, err = r.Get(ctx, "n2")
	require.ErrorIs(t, err, ErrNotFound)

	got, err := r.Get(ctx, "n1")
	require.NoError(t, err)
	require.Equal(t, "b", got.Data["a"])
}

func TestCreateWithoutKey(t *testing.T) {
	r := newTestRepo(t)
	_, created, err := r.Create(context.Background(), sample("n3"), "")
	require.NoError(t, err)
	require.True(t, created)
}
