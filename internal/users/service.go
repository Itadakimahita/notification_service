package users

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"notification-service/internal/domain"
)

const keyPrefix = "user:"

type Store interface {
	Load(ctx context.Context, userID string) (domain.UserData, error)
	PutProfile(ctx context.Context, p domain.Profile) error
	PutPreferences(ctx context.Context, userID string, prefs []domain.Preference) error
	PutDevice(ctx context.Context, userID string, d domain.Device) (string, error)
	DeleteDevice(ctx context.Context, userID, deviceID string) error
	DeactivateDevice(ctx context.Context, userID, deviceID string) error
}

type Service struct {
	store Store
	rdb   *redis.Client
	ttl   time.Duration
	sf    singleflight.Group
}

// NewService wraps the user store with a Redis read-through cache that writes invalidate.
func NewService(store Store, rdb *redis.Client, ttl time.Duration) *Service {
	return &Service{store: store, rdb: rdb, ttl: ttl}
}

// Load returns user data from cache, falling back to the store; unknown users are never cached and cache failures are non-fatal.
func (s *Service) Load(ctx context.Context, userID string) (domain.UserData, error) {
	key := keyPrefix + userID
	if raw, err := s.rdb.Get(ctx, key).Bytes(); err == nil {
		var d domain.UserData
		if gob.NewDecoder(bytes.NewReader(raw)).Decode(&d) == nil {
			return d, nil
		}
	}
	v, err, _ := s.sf.Do(userID, func() (any, error) {
		d, err := s.store.Load(ctx, userID)
		if err != nil {
			return nil, err
		}
		if !d.Found {
			return d, nil
		}
		var buf bytes.Buffer
		if gob.NewEncoder(&buf).Encode(d) == nil {
			_ = s.rdb.Set(ctx, key, buf.Bytes(), s.ttl).Err()
		}
		return d, nil
	})
	if err != nil {
		return domain.UserData{}, err
	}
	return v.(domain.UserData), nil
}

func (s *Service) invalidate(ctx context.Context, userID string) {
	_ = s.rdb.Del(ctx, keyPrefix+userID).Err()
}

// PutProfile stores the profile and invalidates the cache.
func (s *Service) PutProfile(ctx context.Context, p domain.Profile) error {
	err := s.store.PutProfile(ctx, p)
	s.invalidate(ctx, p.UserID)
	return err
}

// PutPreferences stores preferences and invalidates the cache.
func (s *Service) PutPreferences(ctx context.Context, userID string, prefs []domain.Preference) error {
	err := s.store.PutPreferences(ctx, userID, prefs)
	s.invalidate(ctx, userID)
	return err
}

// PutDevice registers a device and invalidates the cache.
func (s *Service) PutDevice(ctx context.Context, userID string, d domain.Device) (string, error) {
	id, err := s.store.PutDevice(ctx, userID, d)
	s.invalidate(ctx, userID)
	return id, err
}

// DeleteDevice removes a device and invalidates the cache.
func (s *Service) DeleteDevice(ctx context.Context, userID, deviceID string) error {
	err := s.store.DeleteDevice(ctx, userID, deviceID)
	s.invalidate(ctx, userID)
	return err
}

// DeactivateDevice disables a rejected token and invalidates the cache.
func (s *Service) DeactivateDevice(ctx context.Context, userID, deviceID string) error {
	err := s.store.DeactivateDevice(ctx, userID, deviceID)
	s.invalidate(ctx, userID)
	return err
}
