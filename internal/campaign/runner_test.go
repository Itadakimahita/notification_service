package campaign

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"notification-service/internal/blob"
	"notification-service/internal/domain"
	"notification-service/internal/store/dynamo"
)

type fakeSource map[string]string

func (f fakeSource) Get(_ context.Context, key string, offset int64) (io.ReadCloser, error) {
	v, ok := f[key]
	if !ok {
		return nil, blob.ErrNotFound
	}
	return io.NopCloser(strings.NewReader(v[offset:])), nil
}

type fakeEmitter struct {
	mu      sync.Mutex
	keys    map[string]int
	notifs  map[string]*domain.Notification
	onEmit  func(total int)
	failFor map[string]error
}

func newEmitter() *fakeEmitter {
	return &fakeEmitter{keys: map[string]int{}, notifs: map[string]*domain.Notification{}}
}

func (f *fakeEmitter) Create(_ context.Context, n *domain.Notification, key string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys[key]++
	created := f.keys[key] == 1
	if created {
		f.notifs[key] = n
	}
	if f.onEmit != nil {
		f.onEmit(len(f.keys))
	}
	return n.ID, created, nil
}

type fakeStore struct {
	mu sync.Mutex
	c  *dynamo.Campaign
}

func (s *fakeStore) Get(context.Context, string) (*dynamo.Campaign, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *s.c
	return &cp, nil
}

func (s *fakeStore) SetStatus(_ context.Context, _ string, from []dynamo.CampaignStatus, to dynamo.CampaignStatus, msg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range from {
		if s.c.Status == f {
			s.c.Status, s.c.Error = to, msg
			return nil
		}
	}
	return dynamo.ErrBadTransition
}

func (s *fakeStore) Progress(_ context.Context, _ string, offset, emitted, skipped int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.c.Offset, s.c.Emitted, s.c.Skipped = offset, emitted, skipped
	return nil
}

func (s *fakeStore) ListRunning(context.Context) ([]dynamo.Campaign, error) { return nil, nil }

type heldLeases struct{}

func (heldLeases) Acquire(context.Context, string, string) (dynamo.Lease, bool, error) {
	return dynamo.Lease{}, true, nil
}
func (heldLeases) Checkpoint(context.Context, string, string, string, bool) (bool, error) {
	return true, nil
}

func setup(csv string, rate int) (*Runner, *fakeStore, *fakeEmitter) {
	st := &fakeStore{c: &dynamo.Campaign{ID: "c1", TemplateID: "promo", Category: "marketing", Channels: []string{"push"},
		Data: map[string]string{"shared": "x", "name": "default"}, RatePerSec: rate, AudienceKey: "a.csv", Status: dynamo.CampaignRunning}}
	em := newEmitter()
	return &Runner{Store: st, Source: fakeSource{"a.csv": csv}, Emitter: em, Leases: heldLeases{}, Owner: "t",
		Log: slog.Default(), Workers: 8, Now: time.Now}, st, em
}

func TestCompletesParsesColumnsAndSkipsBadRows(t *testing.T) {
	csv := "user_id,name\nu1,Alice\n\n,NoUser\nu2\n\"bad\"quote,x\nu3,Carol"
	r, st, em := setup(csv, 1000)
	require.NoError(t, r.RunCampaign(context.Background(), "c1"))

	require.Equal(t, dynamo.CampaignCompleted, st.c.Status)
	require.Len(t, em.keys, 3)
	require.EqualValues(t, 3, st.c.Emitted)
	require.EqualValues(t, 2, st.c.Skipped)

	a := em.notifs["u1"]
	require.Equal(t, "Alice", a.Data["name"], "row column overrides campaign data")
	require.Equal(t, "x", a.Data["shared"])
	require.Equal(t, "default", em.notifs["u2"].Data["name"], "missing column falls back to campaign data")
	require.Equal(t, domain.PriorityLow, a.Priority)
	require.Equal(t, "campaign:c1", a.Producer)
	require.Equal(t, []domain.Channel{domain.ChannelPush}, a.Channels)
}

func TestAudienceWithoutHeader(t *testing.T) {
	r, st, em := setup("u1\nu2\nu3\n", 1000)
	require.NoError(t, r.RunCampaign(context.Background(), "c1"))
	require.Len(t, em.keys, 3)
	require.Equal(t, dynamo.CampaignCompleted, st.c.Status)
}

func TestPauseAndResumeWithoutGapsOrDuplicates(t *testing.T) {
	var lines []string
	for i := 0; i < 24; i++ {
		lines = append(lines, fmt.Sprintf("u%02d,n%d", i, i))
	}
	r, st, em := setup("user_id,name\n"+strings.Join(lines, "\n"), 20)
	em.onEmit = func(total int) {
		if total == 7 {
			st.mu.Lock()
			st.c.Status = dynamo.CampaignPaused
			st.mu.Unlock()
		}
	}
	require.NoError(t, r.RunCampaign(context.Background(), "c1"))
	require.Equal(t, dynamo.CampaignPaused, st.c.Status)
	paused := len(em.keys)
	require.Less(t, paused, 24, "pause must stop before the end")
	require.Positive(t, st.c.Offset)

	st.mu.Lock()
	st.c.Status = dynamo.CampaignRunning
	st.mu.Unlock()
	em.onEmit = nil
	require.NoError(t, r.RunCampaign(context.Background(), "c1"))

	require.Equal(t, dynamo.CampaignCompleted, st.c.Status)
	require.Len(t, em.keys, 24)
	for k, n := range em.keys {
		require.Equal(t, 1, n, "resume re-emitted %s", k)
	}
}

func TestCancelStopsAtChunkBoundary(t *testing.T) {
	var lines []string
	for i := 0; i < 40; i++ {
		lines = append(lines, fmt.Sprintf("u%02d", i))
	}
	r, st, em := setup(strings.Join(lines, "\n"), 20)
	em.onEmit = func(total int) {
		if total == 3 {
			st.mu.Lock()
			st.c.Status = dynamo.CampaignCancelled
			st.mu.Unlock()
		}
	}
	require.NoError(t, r.RunCampaign(context.Background(), "c1"))
	require.Equal(t, dynamo.CampaignCancelled, st.c.Status)
	require.Less(t, len(em.keys), 40)
}

func TestMissingAudienceFailsCampaign(t *testing.T) {
	r, st, _ := setup("", 100)
	r.Source = fakeSource{}
	require.Error(t, r.RunCampaign(context.Background(), "c1"))
	require.Equal(t, dynamo.CampaignFailed, st.c.Status)
	require.Equal(t, "audience file not found", st.c.Error)
}

func TestCrashMidChunkResumesIdempotently(t *testing.T) {
	var lines []string
	for i := 0; i < 30; i++ {
		lines = append(lines, fmt.Sprintf("u%02d", i))
	}
	r, st, em := setup("user_id\n"+strings.Join(lines, "\n"), 20)
	ctx, cancel := context.WithCancel(context.Background())
	em.onEmit = func(total int) {
		if total == 4 {
			cancel()
		}
	}
	_ = r.RunCampaign(ctx, "c1")
	require.Equal(t, dynamo.CampaignRunning, st.c.Status, "a crash must not finish the campaign")

	em.onEmit = nil
	require.NoError(t, r.RunCampaign(context.Background(), "c1"))
	require.Equal(t, dynamo.CampaignCompleted, st.c.Status)
	require.Len(t, em.keys, 30)
}

func TestRateLimitThrottlesEmission(t *testing.T) {
	var lines []string
	for i := 0; i < 30; i++ {
		lines = append(lines, fmt.Sprintf("u%02d", i))
	}
	r, _, em := setup(strings.Join(lines, "\n"), 20)
	start := time.Now()
	require.NoError(t, r.RunCampaign(context.Background(), "c1"))
	elapsed := time.Since(start)
	require.Len(t, em.keys, 30)
	require.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "30 rows at 20/s with burst 10 need at least ~1s")
	require.Less(t, elapsed, 5*time.Second)
}

func mustNotif(campaignID, user string) *domain.Notification {
	return domain.NewNotification("campaign:"+campaignID, user, "promo", domain.CategoryMarketing, domain.PriorityLow, nil, nil, time.Now())
}
