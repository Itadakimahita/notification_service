package users

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"notification-service/internal/domain"
)

type countingStore struct {
	Store
	data  domain.UserData
	loads int
}

func (c *countingStore) Load(context.Context, string) (domain.UserData, error) {
	c.loads++
	return c.data, nil
}

func (c *countingStore) PutProfile(context.Context, domain.Profile) error { return nil }

func testRedis(t *testing.T) *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func TestUnknownUsersAreNotCached(t *testing.T) {
	store := &countingStore{}
	svc := NewService(store, testRedis(t), time.Minute)
	ctx := context.Background()
	id := "ghost-" + time.Now().Format("150405.000000")

	for range 3 {
		d, err := svc.Load(ctx, id)
		require.NoError(t, err)
		require.False(t, d.Found)
	}
	require.Equal(t, 3, store.loads)
}

func TestFoundUsersAreCachedAndInvalidatedOnWrite(t *testing.T) {
	store := &countingStore{data: domain.UserData{Found: true, Profile: domain.Profile{UserID: "u"}, Devices: []domain.Device{{ID: "d", Token: "secret", Active: true}}}}
	svc := NewService(store, testRedis(t), time.Minute)
	ctx := context.Background()
	id := "found-" + time.Now().Format("150405.000000")
	store.data.Profile.UserID = id

	for range 3 {
		d, err := svc.Load(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "secret", d.Devices[0].Token)
	}
	require.Equal(t, 1, store.loads)

	require.NoError(t, svc.PutProfile(ctx, domain.Profile{UserID: id}))
	_, err := svc.Load(ctx, id)
	require.NoError(t, err)
	require.Equal(t, 2, store.loads)
}
