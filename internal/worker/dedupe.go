package worker

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type ClaimState int

const (
	Claimed ClaimState = iota
	AlreadySent
	InFlight
)

type Dedupe interface {
	Claim(ctx context.Context, key string) (ClaimState, error)
	MarkSent(ctx context.Context, key string) error
	Release(ctx context.Context, key string) error
}

const (
	claimTTL = time.Minute
	sentTTL  = 24 * time.Hour
)

type RedisDedupe struct{ RDB *redis.Client }

// Claim reserves a send slot; an existing sent marker means duplicate, a pending one means another worker is mid-send.
func (d RedisDedupe) Claim(ctx context.Context, key string) (ClaimState, error) {
	ok, err := d.RDB.SetNX(ctx, key, "sending", claimTTL).Result()
	if err != nil {
		return 0, err
	}
	if ok {
		return Claimed, nil
	}
	v, err := d.RDB.Get(ctx, key).Result()
	if err == redis.Nil {
		return d.Claim(ctx, key)
	}
	if err != nil {
		return 0, err
	}
	if v == "sent" {
		return AlreadySent, nil
	}
	return InFlight, nil
}

// MarkSent records a successful provider send.
func (d RedisDedupe) MarkSent(ctx context.Context, key string) error {
	return d.RDB.Set(ctx, key, "sent", sentTTL).Err()
}

// Release frees the slot so a retry or replay can send again.
func (d RedisDedupe) Release(ctx context.Context, key string) error {
	return d.RDB.Del(ctx, key).Err()
}
