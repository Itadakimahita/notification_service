package dynamo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"notification-service/internal/domain"
)

func newStatusFixture(t *testing.T) (*Notifications, *Statuses, *dynamodb.Client, Tables) {
	t.Helper()
	endpoint := testEndpoint(t)
	_ = endpoint
	tables, _, _ := testTables(t)
	repo, db := repoFor(t, tables)
	return repo, NewStatuses(db, tables), db, tables
}

func fresh(id string) *domain.Notification {
	n := sample(id)
	n.ChannelStatus, n.ChannelDetail, n.ChannelRank = map[string]domain.Status{}, map[string]string{}, map[string]int{}
	return n
}

func TestStatusesRankGuardAndMaps(t *testing.T) {
	repo, st, _, _ := newStatusFixture(t)
	ctx := context.Background()
	_, _, err := repo.Create(ctx, fresh("n1"), "")
	require.NoError(t, err)

	apply := func(ch string, s domain.Status, detail string) {
		require.NoError(t, st.Apply(ctx, "n1", ch, s, detail))
	}
	apply("push", domain.StatusQueued, "1 target(s)")
	apply("push", domain.StatusSent, "")
	apply("push", domain.StatusQueued, "late duplicate")
	apply("push", domain.StatusDelivered, "")
	apply("push", domain.StatusFailed, "stale failure")
	apply("email", domain.StatusFailed, "retries exhausted")
	apply("email", domain.StatusSent, "replayed")
	apply(AllChannels, domain.StatusRouted, "")

	got, err := repo.Get(ctx, "n1")
	require.NoError(t, err)
	require.Equal(t, domain.StatusDelivered, got.ChannelStatus["push"])
	require.Equal(t, domain.StatusSent, got.ChannelStatus["email"], "replay after FAILED must progress")
	require.Equal(t, domain.StatusRouted, got.Status)
}

func TestStatusesIgnoreUnknownNotification(t *testing.T) {
	_, st, db, tables := newStatusFixture(t)
	ctx := context.Background()
	require.NoError(t, st.Apply(ctx, "ghost", "push", domain.StatusSent, ""))
	require.NoError(t, st.Apply(ctx, "ghost", AllChannels, domain.StatusRouted, ""))
	out, err := db.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(tables.Notifications), Key: map[string]types.AttributeValue{AttrPK: &types.AttributeValueMemberS{Value: "ghost"}}})
	require.NoError(t, err)
	require.Nil(t, out.Item, "status events must never create phantom notifications")
}

func TestStatusesHandleLegacyItemsWithoutMaps(t *testing.T) {
	_, st, db, tables := newStatusFixture(t)
	ctx := context.Background()
	_, err := db.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(tables.Notifications), Item: map[string]types.AttributeValue{
		AttrPK: &types.AttributeValueMemberS{Value: "legacy"}, "status": &types.AttributeValueMemberS{Value: "ACCEPTED"},
	}})
	require.NoError(t, err)
	require.NoError(t, st.Apply(ctx, "legacy", "push", domain.StatusSent, "x"))

	out, err := db.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(tables.Notifications), Key: map[string]types.AttributeValue{AttrPK: &types.AttributeValueMemberS{Value: "legacy"}}})
	require.NoError(t, err)
	cs := out.Item["channel_status"].(*types.AttributeValueMemberM).Value
	require.Equal(t, "SENT", cs["push"].(*types.AttributeValueMemberS).Value)
}

func TestInboxPaginationAndRead(t *testing.T) {
	tables, _, _ := testTables(t)
	_, db := repoFor(t, tables)
	inbox := NewInbox(db, tables)
	ctx := context.Background()
	base := time.Now().Unix()
	for i := 0; i < 5; i++ {
		require.NoError(t, inbox.Put(ctx, "u1", fmt.Sprintf("n%d", i), "t", fmt.Sprintf("body%d", i), base+int64(i)))
	}
	require.NoError(t, inbox.Put(ctx, "u1", "n4", "t", "body4", base+4), "replay must overwrite, not duplicate")

	page1, next, err := inbox.List(ctx, "u1", 3, "")
	require.NoError(t, err)
	require.Len(t, page1, 3)
	require.Equal(t, "body4", page1[0].Body, "newest first")
	require.NotEmpty(t, next)

	page2, next2, err := inbox.List(ctx, "u1", 3, next)
	require.NoError(t, err)
	require.Len(t, page2, 2)
	require.Empty(t, next2)

	require.NoError(t, inbox.MarkRead(ctx, "u1", page1[0].ID))
	page1, _, _ = inbox.List(ctx, "u1", 1, "")
	require.True(t, page1[0].Read)
	require.ErrorIs(t, inbox.MarkRead(ctx, "u1", "000000000000_nope"), ErrNotFound)
	_, _, err = inbox.List(ctx, "u1", 3, "%%%")
	require.Error(t, err)
}
