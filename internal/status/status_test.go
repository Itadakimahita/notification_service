package status

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	pb "notification-service/internal/events/notification/v1"
)

func ev(nid, ch, st string) *pb.StatusEvent {
	return &pb.StatusEvent{NotificationId: nid, Channel: ch, Status: st}
}

func TestCoalesceKeepsHighestRankPerChannel(t *testing.T) {
	out := Coalesce([]*pb.StatusEvent{
		ev("n1", "push", "QUEUED"), ev("n1", "push", "SENT"), ev("n1", "push", "DELIVERED"), ev("n1", "push", "SENT"),
		ev("n1", "email", "QUEUED"), ev("n1", "all", "ROUTED"), ev("n2", "push", "FAILED"),
	})
	got := map[string]string{}
	for _, e := range out {
		got[e.NotificationId+"/"+e.Channel] = e.Status
	}
	require.Equal(t, map[string]string{"n1/push": "DELIVERED", "n1/email": "QUEUED", "n1/all": "ROUTED", "n2/push": "FAILED"}, got)
}

type fakeProducer struct {
	recs []*kgo.Record
	err  error
}

func (f *fakeProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.recs = append(f.recs, rs...)
	return kgo.ProduceResults{{Err: f.err}}
}

func post(h http.Handler, provider, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/v1/webhooks/"+provider, strings.NewReader(body))
	if key != "" {
		req.Header.Set("X-Webhook-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestWebhookAuthValidationAndPublish(t *testing.T) {
	fp := &fakeProducer{}
	h := (&Webhook{Kafka: fp, Keys: map[string]string{"mock": "secret"}, Now: func() time.Time { return time.Unix(100, 0) }}).Router()

	require.Equal(t, 401, post(h, "mock", "", `{}`).Code)
	require.Equal(t, 401, post(h, "mock", "wrong", `{}`).Code)
	require.Equal(t, 401, post(h, "unknown", "secret", `{}`).Code)
	require.Equal(t, 400, post(h, "mock", "secret", `{"channel":"push","status":"delivered"}`).Code)
	require.Equal(t, 400, post(h, "mock", "secret", `{"notification_id":"n","channel":"fax","status":"delivered"}`).Code)
	require.Equal(t, 400, post(h, "mock", "secret", `{"notification_id":"n","channel":"push","status":"read"}`).Code)
	require.Empty(t, fp.recs)

	require.Equal(t, 202, post(h, "mock", "secret", `{"notification_id":"n1","channel":"push","status":"delivered"}`).Code)
	require.Equal(t, 202, post(h, "mock", "secret", `{"notification_id":"n2","channel":"sms","status":"bounced","reason":"no such number"}`).Code)
	require.Len(t, fp.recs, 2)

	var first, second pb.StatusEvent
	require.NoError(t, proto.Unmarshal(fp.recs[0].Value, &first))
	require.NoError(t, proto.Unmarshal(fp.recs[1].Value, &second))
	require.Equal(t, "DELIVERED", first.Status)
	require.Equal(t, "FAILED", second.Status)
	require.Equal(t, "no such number", second.Detail)
	require.Equal(t, "mock", second.Provider)
}

func TestWebhookReportsKafkaFailure(t *testing.T) {
	h := (&Webhook{Kafka: &fakeProducer{err: context.DeadlineExceeded}, Keys: map[string]string{"mock": "k"}, Now: time.Now}).Router()
	require.Equal(t, 503, post(h, "mock", "k", `{"notification_id":"n","channel":"push","status":"delivered"}`).Code)
}
