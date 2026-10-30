package status

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"notification-service/internal/domain"
	pb "notification-service/internal/events/notification/v1"
	"notification-service/internal/httpx"
	"notification-service/internal/kafkax"
)

type webhookBody struct {
	NotificationID    string `json:"notification_id"`
	Channel           string `json:"channel"`
	ProviderMessageID string `json:"provider_message_id"`
	Status            string `json:"status"`
	Reason            string `json:"reason"`
}

type Producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

type Webhook struct {
	Kafka Producer
	Keys  map[string]string
	Now   func() time.Time
}

// Router returns the webhook HTTP handler tree.
func (w *Webhook) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(httpx.Metrics)
	r.Post("/v1/webhooks/{provider}", w.receive)
	return r
}

// receive validates a provider callback and publishes it as a status event before acknowledging.
func (w *Webhook) receive(rw http.ResponseWriter, r *http.Request) {
	want, ok := w.Keys[chi.URLParam(r, "provider")]
	got := r.Header.Get("X-Webhook-Key")
	if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		httpx.Error(rw, http.StatusUnauthorized, "invalid webhook key")
		return
	}
	var body webhookBody
	if err := json.NewDecoder(http.MaxBytesReader(rw, r.Body, 1<<20)).Decode(&body); err != nil || body.NotificationID == "" || !domain.Channel(body.Channel).Valid() {
		httpx.Error(rw, http.StatusBadRequest, "notification_id and valid channel are required")
		return
	}
	var st domain.Status
	switch body.Status {
	case "delivered":
		st = domain.StatusDelivered
	case "failed", "bounced":
		st = domain.StatusFailed
	default:
		httpx.Error(rw, http.StatusBadRequest, "status must be delivered, failed or bounced")
		return
	}
	detail := body.Reason
	if detail == "" && st == domain.StatusFailed {
		detail = "provider reported " + body.Status
	}
	val, _ := proto.Marshal(&pb.StatusEvent{
		NotificationId: body.NotificationID, Channel: body.Channel, Status: string(st),
		Detail: detail, Provider: chi.URLParam(r, "provider"), At: w.Now().Unix(),
	})
	rec := &kgo.Record{Topic: kafkax.TopicStatus, Key: []byte(body.NotificationID), Value: val}
	if err := w.Kafka.ProduceSync(r.Context(), rec).FirstErr(); err != nil {
		httpx.Error(rw, http.StatusServiceUnavailable, "could not record status")
		return
	}
	rw.WriteHeader(http.StatusAccepted)
}
