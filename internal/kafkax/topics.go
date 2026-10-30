package kafkax

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"notification-service/internal/domain"
)

const (
	TopicRequested = "notifications.requested"
	TopicStatus    = "notifications.status"
)

var RetryTiers = []time.Duration{5 * time.Second, 30 * time.Second, 5 * time.Minute, 30 * time.Minute}

type TopicSpec struct {
	Name       string
	Partitions int32
	Retention  time.Duration
}

// DispatchTopic returns the per channel/priority lane topic name.
func DispatchTopic(c domain.Channel, p domain.Priority) string {
	return fmt.Sprintf("notifications.dispatch.%s.%s", c, p)
}

// RetryTopic returns the retry topic for a channel and delay tier.
func RetryTopic(c domain.Channel, delay time.Duration) string {
	return fmt.Sprintf("notifications.retry.%s.%s", c, delay)
}

// DLQTopic returns the dead-letter topic for a channel.
func DLQTopic(c domain.Channel) string {
	return fmt.Sprintf("notifications.dlq.%s", c)
}

// AllTopics lists every topic the platform needs with its sizing.
func AllTopics() []TopicSpec {
	week := 7 * 24 * time.Hour
	specs := []TopicSpec{
		{TopicRequested, 48, week},
		{TopicStatus, 24, week},
	}
	for _, c := range domain.Channels {
		specs = append(specs,
			TopicSpec{DispatchTopic(c, domain.PriorityHigh), 48, week},
			TopicSpec{DispatchTopic(c, domain.PriorityLow), 24, week},
			TopicSpec{DLQTopic(c), 6, 2 * week},
		)
		for _, d := range RetryTiers {
			specs = append(specs, TopicSpec{RetryTopic(c, d), 12, week})
		}
	}
	return specs
}

// EnsureTopics creates missing topics and leaves existing ones untouched.
func EnsureTopics(ctx context.Context, adm *kadm.Client, specs []TopicSpec, replication int16) error {
	for _, s := range specs {
		retention := fmt.Sprint(s.Retention.Milliseconds())
		cfg := map[string]*string{"retention.ms": &retention}
		resp, err := adm.CreateTopic(ctx, s.Partitions, replication, cfg, s.Name)
		if errors.Is(err, kerr.TopicAlreadyExists) || errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			continue
		}
		if err != nil {
			return fmt.Errorf("create %s: %w", s.Name, err)
		}
		if resp.Err != nil {
			return fmt.Errorf("create %s: %w", s.Name, resp.Err)
		}
	}
	return nil
}
