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

func testTables(t *testing.T) (Tables, *Users, *Templates) {
	t.Helper()
	endpoint := os.Getenv("DYNAMO_ENDPOINT")
	if endpoint == "" {
		t.Skip("DYNAMO_ENDPOINT not set")
	}
	c := config.Dynamo{Endpoint: endpoint, Region: "us-east-1", TablePrefix: fmt.Sprintf("ut%d_", time.Now().UnixNano())}
	awsCfg, err := LoadAWS(context.Background(), c)
	require.NoError(t, err)
	db := NewClient(awsCfg, c)
	tables := NewTables(c.TablePrefix)
	require.NoError(t, EnsureTables(context.Background(), db, tables))
	return tables, NewUsers(db, tables), NewTemplates(db, tables)
}

func TestUsersRoundTrip(t *testing.T) {
	_, users, _ := testTables(t)
	ctx := context.Background()

	data, err := users.Load(ctx, "ghost")
	require.NoError(t, err)
	require.False(t, data.Found)

	require.NoError(t, users.PutProfile(ctx, domain.Profile{UserID: "u1", Locale: "kk-KZ", Timezone: "Asia/Almaty", Email: "a@b.c"}))
	require.NoError(t, users.PutPreferences(ctx, "u1", []domain.Preference{
		{Category: domain.CategoryMarketing, Channel: domain.ChannelEmail, Enabled: false},
		{Category: domain.CategoryMarketing, Channel: domain.ChannelPush, Enabled: true, QuietStart: "22:00", QuietEnd: "08:00"},
	}))
	id, err := users.PutDevice(ctx, "u1", domain.Device{Token: "tok", Platform: "ios"})
	require.NoError(t, err)

	data, err = users.Load(ctx, "u1")
	require.NoError(t, err)
	require.True(t, data.Found)
	require.Equal(t, "kk-KZ", data.Profile.Locale)
	require.Len(t, data.Prefs, 2)
	require.Len(t, data.ActiveDevices(), 1)
	require.Equal(t, "tok", data.Devices[0].Token)

	require.NoError(t, users.DeactivateDevice(ctx, "u1", id))
	data, _ = users.Load(ctx, "u1")
	require.Empty(t, data.ActiveDevices())

	require.NoError(t, users.DeleteDevice(ctx, "u1", id))
	data, _ = users.Load(ctx, "u1")
	require.Empty(t, data.Devices)
}

func TestTemplateVersioning(t *testing.T) {
	_, _, tpls := testTables(t)
	ctx := context.Background()

	_, err := tpls.GetLatest(ctx, "otp", domain.ChannelSMS, "en")
	require.ErrorIs(t, err, ErrNotFound)

	v1, err := tpls.Put(ctx, domain.Template{ID: "otp", Channel: domain.ChannelSMS, Locale: "en", Body: "v1"})
	require.NoError(t, err)
	v2, err := tpls.Put(ctx, domain.Template{ID: "otp", Channel: domain.ChannelSMS, Locale: "en", Body: "v2"})
	require.NoError(t, err)
	require.Equal(t, 1, v1.Version)
	require.Equal(t, 2, v2.Version)

	latest, err := tpls.GetLatest(ctx, "otp", domain.ChannelSMS, "en")
	require.NoError(t, err)
	require.Equal(t, "v2", latest.Body)

	_, err = tpls.GetLatest(ctx, "otp", domain.ChannelSMS, "en-US")
	require.ErrorIs(t, err, ErrNotFound)
}
