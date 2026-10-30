package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"notification-service/internal/channels"
	"notification-service/internal/domain"
	pb "notification-service/internal/events/notification/v1"
	"notification-service/internal/kafkax"
)

var fixedNow = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

type memDedupe struct {
	mu sync.Mutex
	m  map[string]string
}

func newMem() *memDedupe { return &memDedupe{m: map[string]string{}} }

func (d *memDedupe) Claim(_ context.Context, k string) (ClaimState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch d.m[k] {
	case "":
		d.m[k] = "sending"
		return Claimed, nil
	case "sent":
		return AlreadySent, nil
	}
	return InFlight, nil
}
func (d *memDedupe) MarkSent(_ context.Context, k string) error { d.m[k] = "sent"; return nil }
func (d *memDedupe) Release(_ context.Context, k string) error  { delete(d.m, k); return nil }

type fakeSender struct {
	calls int
	err   error
	res   channels.Result
}

func (f *fakeSender) Send(context.Context, *pb.DispatchMessage) (channels.Result, error) {
	f.calls++
	return f.res, f.err
}

type fakeLimiter struct{ err error }

func (f fakeLimiter) Wait(context.Context) error { return f.err }

type fakeDevices struct{ user, device string }

func (f *fakeDevices) DeactivateDevice(_ context.Context, u, d string) error {
	f.user, f.device = u, d
	return nil
}

func newWorker(s *fakeSender) (*Worker, *memDedupe, *fakeDevices) {
	dd, dev := newMem(), &fakeDevices{}
	return &Worker{
		Channel: domain.ChannelPush, Sender: s, Dedupe: dd, Devices: dev,
		DeviceID: func(tok string) string { return "id-" + tok },
		Limiters: map[domain.Priority]Limiter{domain.PriorityHigh: fakeLimiter{}},
		Breaker:  NewBreaker("t"), MaxAttempts: 5, SendTimeout: time.Second, LimitWait: time.Second,
		Now: func() time.Time { return fixedNow },
	}, dd, dev
}

func msg(attempt int32) *pb.DispatchMessage {
	return &pb.DispatchMessage{NotificationId: "n1", UserId: "u1", Channel: "push", Target: "tok", Priority: "high", Attempt: attempt}
}

func statuses(r Result) []string {
	var out []string
	for _, s := range r.Status {
		out = append(out, s.Status)
	}
	return out
}

func TestSuccessThenDuplicateIsSkipped(t *testing.T) {
	s := &fakeSender{res: channels.Result{Provider: "p"}}
	w, _, _ := newWorker(s)
	r, err := w.Process(context.Background(), msg(0))
	require.NoError(t, err)
	require.Equal(t, []string{"SENT"}, statuses(r))

	r, err = w.Process(context.Background(), msg(0))
	require.NoError(t, err)
	require.Empty(t, r.Status)
	require.Equal(t, 1, s.calls)
}

func TestDeliveredImmediatelyForInbox(t *testing.T) {
	w, _, _ := newWorker(&fakeSender{res: channels.Result{Delivered: true}})
	r, _ := w.Process(context.Background(), msg(0))
	require.Equal(t, []string{"SENT", "DELIVERED"}, statuses(r))
}

func TestTransientFailureWalksRetryTiersThenDeadLetters(t *testing.T) {
	w, dd, _ := newWorker(&fakeSender{err: errors.New("503")})
	for attempt, want := range kafkax.RetryTiers {
		r, err := w.Process(context.Background(), msg(int32(attempt)))
		require.NoError(t, err)
		require.NotNil(t, r.Retry, "attempt %d", attempt)
		require.Equal(t, want, r.Retry.Delay)
		require.EqualValues(t, attempt+1, r.Retry.Msg.Attempt)
		require.Empty(t, dd.m, "claim must be released so the retry can send")
	}
	r, err := w.Process(context.Background(), msg(4))
	require.NoError(t, err)
	require.Nil(t, r.Retry)
	require.NotNil(t, r.Dead)
	require.Equal(t, []string{"FAILED"}, statuses(r))
}

func TestPermanentFailureIsNotRetried(t *testing.T) {
	w, dd, _ := newWorker(&fakeSender{err: channels.Permanent(errors.New("400"))})
	r, err := w.Process(context.Background(), msg(0))
	require.NoError(t, err)
	require.Nil(t, r.Retry)
	require.Nil(t, r.Dead)
	require.Equal(t, []string{"FAILED"}, statuses(r))
	require.Empty(t, dd.m)
}

func TestInvalidTokenDeactivatesDevice(t *testing.T) {
	w, _, dev := newWorker(&fakeSender{err: channels.ErrInvalidToken})
	r, err := w.Process(context.Background(), msg(0))
	require.NoError(t, err)
	require.Equal(t, []string{"FAILED"}, statuses(r))
	require.Equal(t, "u1", dev.user)
	require.Equal(t, "id-tok", dev.device)
}

func TestExpiredIsNotSent(t *testing.T) {
	s := &fakeSender{}
	w, _, _ := newWorker(s)
	m := msg(0)
	m.ExpiresAt = fixedNow.Unix() - 1
	r, _ := w.Process(context.Background(), m)
	require.Equal(t, []string{"EXPIRED"}, statuses(r))
	require.Zero(t, s.calls)
}

func TestInFlightClaimRetriesWithoutBurningAttempt(t *testing.T) {
	s := &fakeSender{}
	w, dd, _ := newWorker(s)
	dd.m["sent:n1:push:tok"] = "sending"
	r, err := w.Process(context.Background(), msg(2))
	require.NoError(t, err)
	require.NotNil(t, r.Retry)
	require.EqualValues(t, 2, r.Retry.Msg.Attempt)
	require.Zero(t, s.calls)
}

func TestRateLimitTimeoutRequeuesAndReleases(t *testing.T) {
	s := &fakeSender{}
	w, dd, _ := newWorker(s)
	w.Limiters[domain.PriorityHigh] = fakeLimiter{err: context.DeadlineExceeded}
	r, err := w.Process(context.Background(), msg(1))
	require.NoError(t, err)
	require.NotNil(t, r.Retry)
	require.EqualValues(t, 1, r.Retry.Msg.Attempt)
	require.Zero(t, s.calls)
	require.Empty(t, dd.m)
}

func TestBreakerOpensAndStopsCallingProvider(t *testing.T) {
	s := &fakeSender{err: errors.New("503")}
	w, _, _ := newWorker(s)
	for i := 0; i < 25; i++ {
		_, _ = w.Process(context.Background(), msg(0))
	}
	calls := s.calls
	require.Less(t, calls, 25)

	r, err := w.Process(context.Background(), msg(1))
	require.NoError(t, err)
	require.Equal(t, calls, s.calls)
	require.NotNil(t, r.Retry)
	require.EqualValues(t, 1, r.Retry.Msg.Attempt)
	require.Equal(t, kafkax.RetryTiers[1], r.Retry.Delay)
}
