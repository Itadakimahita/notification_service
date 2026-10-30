package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sony/gobreaker/v2"
	"google.golang.org/protobuf/proto"

	"notification-service/internal/channels"
	"notification-service/internal/domain"
	pb "notification-service/internal/events/notification/v1"
	"notification-service/internal/kafkax"
)

var (
	sendTotal = promauto.NewCounterVec(prometheus.CounterOpts{Name: "worker_send_total", Help: "Send outcomes."}, []string{"channel", "result"})
	sendTime  = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "worker_send_seconds", Help: "Provider call latency.", Buckets: prometheus.DefBuckets}, []string{"channel"})
)

type Limiter interface {
	Wait(ctx context.Context) error
}

type Devices interface {
	DeactivateDevice(ctx context.Context, userID, deviceID string) error
}

type Retry struct {
	Msg   *pb.DispatchMessage
	Delay time.Duration
}

type Dead struct {
	Msg    *pb.DispatchMessage
	Reason string
}

type Result struct {
	Status []*pb.StatusEvent
	Retry  *Retry
	Dead   *Dead
}

type Worker struct {
	Channel     domain.Channel
	Sender      channels.Sender
	Dedupe      Dedupe
	Limiters    map[domain.Priority]Limiter
	Breaker     *gobreaker.CircuitBreaker[channels.Result]
	Devices     Devices
	DeviceID    func(token string) string
	MaxAttempts int
	SendTimeout time.Duration
	LimitWait   time.Duration
	Now         func() time.Time
}

// NewBreaker builds a provider circuit breaker that only counts retryable failures.
func NewBreaker(name string) *gobreaker.CircuitBreaker[channels.Result] {
	return gobreaker.NewCircuitBreaker[channels.Result](gobreaker.Settings{
		Name:        name,
		MaxRequests: 5,
		Interval:    30 * time.Second,
		Timeout:     15 * time.Second,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.Requests >= 20 && float64(c.TotalFailures)/float64(c.Requests) >= 0.5
		},
		IsSuccessful: func(err error) bool { return err == nil || channels.IsPermanent(err) },
	})
}

func dedupeKey(m *pb.DispatchMessage) string {
	return fmt.Sprintf("sent:%s:%s:%s", m.NotificationId, m.Channel, m.Target)
}

// Process sends one message and decides its fate; the returned error means infrastructure failed and the call should be repeated.
func (w *Worker) Process(ctx context.Context, msg *pb.DispatchMessage) (Result, error) {
	now := w.Now()
	var res Result
	status := func(st domain.Status, detail, provider string) {
		res.Status = append(res.Status, &pb.StatusEvent{
			NotificationId: msg.NotificationId, Channel: msg.Channel, Status: string(st),
			Detail: detail, Provider: provider, Attempt: msg.Attempt, At: now.Unix(),
		})
	}
	outcome := func(r string) { sendTotal.WithLabelValues(msg.Channel, r).Inc() }

	if msg.ExpiresAt > 0 && now.Unix() >= msg.ExpiresAt {
		status(domain.StatusExpired, "expired before send", "")
		outcome("expired")
		return res, nil
	}

	key := dedupeKey(msg)
	state, err := w.Dedupe.Claim(ctx, key)
	if err != nil {
		return res, fmt.Errorf("dedupe claim: %w", err)
	}
	switch state {
	case AlreadySent:
		outcome("duplicate")
		return res, nil
	case InFlight:
		outcome("in_flight")
		res.Retry = &Retry{Msg: retryCopy(msg, msg.Attempt), Delay: kafkax.RetryTiers[0]}
		return res, nil
	}

	release := func() { _ = w.Dedupe.Release(context.WithoutCancel(ctx), key) }

	prio := domain.Priority(msg.Priority)
	if l, ok := w.Limiters[prio]; ok {
		waitCtx, cancel := context.WithTimeout(ctx, w.LimitWait)
		err := l.Wait(waitCtx)
		cancel()
		if err != nil {
			release()
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			outcome("rate_limited")
			res.Retry = &Retry{Msg: retryCopy(msg, msg.Attempt), Delay: kafkax.RetryTiers[0]}
			return res, nil
		}
	}

	start := time.Now()
	sendCtx, cancel := context.WithTimeout(ctx, w.SendTimeout)
	result, err := w.Breaker.Execute(func() (channels.Result, error) { return w.Sender.Send(sendCtx, msg) })
	cancel()
	sendTime.WithLabelValues(msg.Channel).Observe(time.Since(start).Seconds())

	switch {
	case err == nil:
		if err := w.Dedupe.MarkSent(context.WithoutCancel(ctx), key); err != nil {
			return res, fmt.Errorf("dedupe mark: %w", err)
		}
		status(domain.StatusSent, "", result.Provider)
		if result.Delivered {
			status(domain.StatusDelivered, "", result.Provider)
		}
		outcome("sent")

	case errors.Is(err, channels.ErrInvalidToken):
		release()
		if w.Channel == domain.ChannelPush && w.Devices != nil {
			if derr := w.Devices.DeactivateDevice(context.WithoutCancel(ctx), msg.UserId, w.DeviceID(msg.Target)); derr != nil {
				return res, fmt.Errorf("deactivate device: %w", derr)
			}
		}
		status(domain.StatusFailed, "invalid token", result.Provider)
		outcome("invalid_token")

	case channels.IsPermanent(err):
		release()
		status(domain.StatusFailed, err.Error(), result.Provider)
		outcome("permanent")

	case errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests):
		release()
		outcome("breaker_open")
		res.Retry = &Retry{Msg: retryCopy(msg, msg.Attempt), Delay: kafkax.RetryTiers[1]}

	default:
		release()
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		next := int(msg.Attempt) + 1
		if next >= w.MaxAttempts {
			res.Dead = &Dead{Msg: retryCopy(msg, int32(next)), Reason: err.Error()}
			status(domain.StatusFailed, "retries exhausted: "+err.Error(), result.Provider)
			outcome("dead")
			break
		}
		res.Retry = &Retry{Msg: retryCopy(msg, int32(next)), Delay: kafkax.RetryTiers[min(next-1, len(kafkax.RetryTiers)-1)]}
		outcome("retry")
	}
	return res, nil
}

func retryCopy(m *pb.DispatchMessage, attempt int32) *pb.DispatchMessage {
	c := proto.Clone(m).(*pb.DispatchMessage)
	c.Attempt = attempt
	return c
}
