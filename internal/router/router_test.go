package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"notification-service/internal/domain"
	pb "notification-service/internal/events/notification/v1"
	"notification-service/internal/store/dynamo"
	"notification-service/internal/templates"
)

type fakeUsers struct{ data domain.UserData }

func (f fakeUsers) Load(context.Context, string) (domain.UserData, error) { return f.data, nil }

type fakeSource map[string]domain.Template

func (f fakeSource) GetLatest(_ context.Context, id string, ch domain.Channel, locale string) (*domain.Template, error) {
	if t, ok := f[id+"|"+string(ch)+"|"+locale]; ok {
		return &t, nil
	}
	return nil, dynamo.ErrNotFound
}

type fakeCaps struct{ allow bool }

func (f fakeCaps) Allow(context.Context, string, domain.Category, string, time.Time) (bool, error) {
	return f.allow, nil
}

var now = time.Date(2026, 6, 1, 23, 30, 0, 0, time.UTC)

func newRouter(user domain.UserData, caps Caps) *Router {
	src := fakeSource{}
	for _, ch := range domain.Channels {
		src["promo|"+string(ch)+"|en"] = domain.Template{ID: "promo", Channel: ch, Locale: "en", Subject: "Hi {{.name}}", Body: "Sale for {{.name}}"}
		src["promo|"+string(ch)+"|kk"] = domain.Template{ID: "promo", Channel: ch, Locale: "kk", Subject: "Сәлем {{.name}}", Body: "Жеңілдік {{.name}}"}
	}
	return &Router{
		Users:     fakeUsers{user},
		Templates: templates.NewService(src, time.Minute),
		Caps:      caps,
		Now:       func() time.Time { return now },
	}
}

func user() domain.UserData {
	return domain.UserData{
		Found:   true,
		Profile: domain.Profile{UserID: "u1", Locale: "kk-KZ", Timezone: "UTC", Email: "a@b.c", Phone: "+7700"},
		Devices: []domain.Device{{ID: "d1", Token: "t1", Platform: "ios", Active: true}, {ID: "d2", Token: "t2", Platform: "android", Active: true}, {ID: "d3", Token: "t3", Active: false}},
	}
}

func request(mods ...func(*pb.NotificationRequested)) *pb.NotificationRequested {
	r := &pb.NotificationRequested{NotificationId: "n1", UserId: "u1", TemplateId: "promo", Category: "marketing", Priority: "low",
		Data: map[string]string{"name": "Dias"}, CreatedAt: now.Unix()}
	for _, m := range mods {
		m(r)
	}
	return r
}

func statusOf(o Outcome, ch string) string {
	for _, s := range o.Status {
		if s.Channel == ch {
			return s.Status + ":" + s.Detail
		}
	}
	return ""
}

func TestFanOutDefaultChannelsWithLocaleFallback(t *testing.T) {
	out, err := newRouter(user(), fakeCaps{true}).Route(context.Background(), request())
	require.NoError(t, err)
	require.Len(t, out.Dispatch, 4)
	targets := map[string]int{}
	for _, d := range out.Dispatch {
		targets[d.Channel]++
		require.Equal(t, "Жеңілдік Dias", d.Body)
	}
	require.Equal(t, map[string]int{"push": 2, "email": 1, "inapp": 1}, targets)
	require.Equal(t, "ROUTED:", statusOf(out, "all"))
}

func TestExpired(t *testing.T) {
	out, err := newRouter(user(), nil).Route(context.Background(), request(func(r *pb.NotificationRequested) { r.ExpiresAt = now.Unix() - 1 }))
	require.NoError(t, err)
	require.Empty(t, out.Dispatch)
	require.Contains(t, statusOf(out, "all"), "EXPIRED")
}

func TestSendAtDefersAndReleasedSkipsDeferral(t *testing.T) {
	future := now.Add(time.Hour).Unix()
	out, err := newRouter(user(), nil).Route(context.Background(), request(func(r *pb.NotificationRequested) { r.SendAt = future }))
	require.NoError(t, err)
	require.Empty(t, out.Dispatch)
	require.Len(t, out.Deferred, 1)
	require.Equal(t, future, out.Deferred[0].At.Unix())

	out, err = newRouter(user(), nil).Route(context.Background(), request(func(r *pb.NotificationRequested) { r.SendAt = future; r.Released = true }))
	require.NoError(t, err)
	require.NotEmpty(t, out.Dispatch)
	require.Empty(t, out.Deferred)
}

func TestUnknownUser(t *testing.T) {
	out, err := newRouter(domain.UserData{}, nil).Route(context.Background(), request())
	require.NoError(t, err)
	require.Contains(t, statusOf(out, "all"), "unknown user")
}

func TestOptOutAndMissingTargets(t *testing.T) {
	u := user()
	u.Profile.Email = ""
	u.Prefs = []domain.Preference{{Category: domain.CategoryMarketing, Channel: domain.ChannelPush, Enabled: false}}
	out, err := newRouter(u, fakeCaps{true}).Route(context.Background(), request())
	require.NoError(t, err)
	require.Equal(t, "SUPPRESSED:opted out", statusOf(out, "push"))
	require.Equal(t, "SUPPRESSED:no email address", statusOf(out, "email"))
	require.Len(t, out.Dispatch, 1)
	require.Equal(t, "inapp", out.Dispatch[0].Channel)
}

func TestQuietHoursDeferMarketingButNotTransactional(t *testing.T) {
	u := user()
	u.Prefs = []domain.Preference{{Category: domain.CategoryMarketing, Channel: domain.ChannelPush, Enabled: true, QuietStart: "22:00", QuietEnd: "08:00"}}
	out, err := newRouter(u, fakeCaps{true}).Route(context.Background(), request(func(r *pb.NotificationRequested) { r.Channels = []string{"push"} }))
	require.NoError(t, err)
	require.Empty(t, out.Dispatch)
	require.Len(t, out.Deferred, 1)
	require.Equal(t, time.Date(2026, 6, 2, 8, 0, 0, 0, time.UTC), out.Deferred[0].At)

	out, err = newRouter(u, fakeCaps{true}).Route(context.Background(), request(func(r *pb.NotificationRequested) {
		r.Channels = []string{"push"}
		r.Category = "transactional"
	}))
	require.NoError(t, err)
	require.Len(t, out.Dispatch, 2)
	require.Empty(t, out.Deferred)
}

func TestFrequencyCapSuppressesAll(t *testing.T) {
	out, err := newRouter(user(), fakeCaps{false}).Route(context.Background(), request())
	require.NoError(t, err)
	require.Empty(t, out.Dispatch)
	require.Equal(t, "SUPPRESSED:frequency cap", statusOf(out, "push"))
}

func TestTemplateNotFoundFailsChannel(t *testing.T) {
	out, err := newRouter(user(), fakeCaps{true}).Route(context.Background(), request(func(r *pb.NotificationRequested) { r.TemplateId = "missing" }))
	require.NoError(t, err)
	require.Empty(t, out.Dispatch)
	require.Equal(t, "FAILED:template not found", statusOf(out, "push"))
}

func TestMissingTemplateVariableFailsChannelOnly(t *testing.T) {
	out, err := newRouter(user(), fakeCaps{true}).Route(context.Background(), request(func(r *pb.NotificationRequested) { r.Data = nil }))
	require.NoError(t, err)
	require.Empty(t, out.Dispatch)
	require.Contains(t, statusOf(out, "email"), "FAILED:template render")
}

type failingUsers struct{}

func (failingUsers) Load(context.Context, string) (domain.UserData, error) {
	return domain.UserData{}, errors.New("boom")
}

func TestInfrastructureErrorIsReturnedForRetry(t *testing.T) {
	r := newRouter(user(), nil)
	r.Users = failingUsers{}
	_, err := r.Route(context.Background(), request())
	require.Error(t, err)
}
