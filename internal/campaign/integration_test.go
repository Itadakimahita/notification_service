package campaign

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/require"

	"notification-service/internal/blob"
	"notification-service/internal/blob/blobtest"
	"notification-service/internal/config"
	"notification-service/internal/store/dynamo"
)

func TestRunnerAgainstDynamoAndS3Protocol(t *testing.T) {
	endpoint := os.Getenv("DYNAMO_ENDPOINT")
	if endpoint == "" {
		t.Skip("DYNAMO_ENDPOINT not set")
	}
	ctx := context.Background()
	dc := config.Dynamo{Endpoint: endpoint, Region: "us-east-1", TablePrefix: fmt.Sprintf("c%d_", time.Now().UnixNano())}
	awsCfg, err := dynamo.LoadAWS(ctx, dc)
	require.NoError(t, err)
	db := dynamo.NewClient(awsCfg, dc)
	tables := dynamo.NewTables(dc.TablePrefix)
	require.NoError(t, dynamo.EnsureTables(ctx, db, tables))

	s3srv := httptest.NewServer(blobtest.NewFakeS3())
	defer s3srv.Close()
	store := blob.New(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("k", "s", "")}, s3srv.URL, "bkt")

	var rows []string
	for i := 0; i < 120; i++ {
		rows = append(rows, fmt.Sprintf("user%03d,Name%d", i, i))
	}
	require.NoError(t, store.Put(ctx, "campaigns/c1/audience.csv", strings.NewReader("user_id,name\n"+strings.Join(rows, "\n")+"\n")))

	campaigns := dynamo.NewCampaigns(db, tables)
	notifs := dynamo.NewNotifications(db, tables)
	c := &dynamo.Campaign{ID: "c1", Name: "n", TemplateID: "promo", Category: "marketing", Channels: []string{"push"},
		RatePerSec: 400, AudienceKey: "campaigns/c1/audience.csv", Status: dynamo.CampaignDraft}
	require.NoError(t, campaigns.Create(ctx, c))
	require.ErrorIs(t, campaigns.SetStatus(ctx, "c1", []dynamo.CampaignStatus{dynamo.CampaignPaused}, dynamo.CampaignRunning, ""), dynamo.ErrBadTransition)
	require.NoError(t, campaigns.SetStatus(ctx, "c1", []dynamo.CampaignStatus{dynamo.CampaignDraft}, dynamo.CampaignRunning, ""))

	running, err := campaigns.ListRunning(ctx)
	require.NoError(t, err)
	require.Len(t, running, 1)

	r := &Runner{Store: campaigns, Source: store, Emitter: notifs, Leases: dynamo.NewLeases(db, tables, 30*time.Second),
		Owner: "t", Log: slog.Default(), Workers: 16, Now: time.Now}
	_, ok, err := r.Leases.Acquire(ctx, "campaign#c1", "t")
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, r.RunCampaign(ctx, "c1"))

	got, err := campaigns.Get(ctx, "c1")
	require.NoError(t, err)
	require.Equal(t, dynamo.CampaignCompleted, got.Status)
	require.EqualValues(t, 120, got.Emitted)

	// rerunning the same audience must not create a second notification per user (idempotency)
	require.NoError(t, campaigns.SetStatus(ctx, "c1", []dynamo.CampaignStatus{dynamo.CampaignCompleted}, dynamo.CampaignRunning, ""))
	require.NoError(t, campaigns.Progress(ctx, "c1", 0, 0, 0))
	require.NoError(t, r.RunCampaign(ctx, "c1"))
	id1, created, err := notifs.Create(ctx, mustNotif("c1", "user007"), "user007")
	require.NoError(t, err)
	require.False(t, created, "idempotency record from the first run must still be there")
	n, err := notifs.Get(ctx, id1)
	require.NoError(t, err)
	require.Equal(t, "campaign:c1", n.Producer)
	require.Equal(t, "Name7", n.Data["name"])
}
