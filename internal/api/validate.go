package api

import (
	"errors"
	"fmt"
	"time"

	"notification-service/internal/domain"
)

const (
	maxDataBytes  = 16 * 1024
	maxScheduleIn = 90 * 24 * time.Hour
)

type createRequest struct {
	UserID         string            `json:"user_id"`
	TemplateID     string            `json:"template_id"`
	Category       domain.Category   `json:"category"`
	Priority       domain.Priority   `json:"priority"`
	Channels       []domain.Channel  `json:"channels"`
	Data           map[string]string `json:"data"`
	SendAt         *time.Time        `json:"send_at"`
	ExpiresAt      *time.Time        `json:"expires_at"`
	IdempotencyKey string            `json:"idempotency_key"`
}

// build validates the request and converts it into a notification.
func (r createRequest) build(producer string, now time.Time) (*domain.Notification, error) {
	if r.UserID == "" || len(r.UserID) > 128 {
		return nil, errors.New("user_id is required (max 128 chars)")
	}
	if r.TemplateID == "" || len(r.TemplateID) > 128 {
		return nil, errors.New("template_id is required (max 128 chars)")
	}
	if !r.Category.Valid() {
		return nil, errors.New("category must be transactional or marketing")
	}
	if r.Priority == "" {
		r.Priority = domain.PriorityLow
		if r.Category == domain.CategoryTransactional {
			r.Priority = domain.PriorityHigh
		}
	}
	if !r.Priority.Valid() {
		return nil, errors.New("priority must be high or low")
	}
	seen := map[domain.Channel]bool{}
	var channels []domain.Channel
	for _, c := range r.Channels {
		if !c.Valid() {
			return nil, fmt.Errorf("unknown channel %q", c)
		}
		if !seen[c] {
			seen[c] = true
			channels = append(channels, c)
		}
	}
	size := 0
	for k, v := range r.Data {
		size += len(k) + len(v)
	}
	if size > maxDataBytes {
		return nil, errors.New("data exceeds 16KB")
	}

	n := domain.NewNotification(producer, r.UserID, r.TemplateID, r.Category, r.Priority, channels, r.Data, now)
	if r.SendAt != nil {
		if r.SendAt.After(now.Add(maxScheduleIn)) {
			return nil, errors.New("send_at is more than 90 days ahead")
		}
		if r.SendAt.After(now) {
			n.SendAt = r.SendAt.Unix()
		}
	}
	if r.ExpiresAt != nil {
		if !r.ExpiresAt.After(now) || r.ExpiresAt.Unix() <= n.SendAt {
			return nil, errors.New("expires_at must be in the future and after send_at")
		}
		n.ExpiresAt = r.ExpiresAt.Unix()
	}
	n.ComputeTTL()
	return n, nil
}
