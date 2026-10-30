package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	pb "notification-service/internal/events/notification/v1"
)

type HTTP struct {
	client   *http.Client
	url      string
	provider string
	payload  func(*pb.DispatchMessage) any
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

// NewPush sends push notifications to an FCM/APNs-style HTTP provider.
func NewPush(baseURL string) *HTTP {
	return &HTTP{client: newHTTPClient(), url: baseURL + "/push/send", provider: "mock-push",
		payload: func(m *pb.DispatchMessage) any {
			return map[string]string{"token": m.Target, "platform": m.Platform, "title": m.Title, "body": m.Body, "notification_id": m.NotificationId, "collapse_id": m.NotificationId}
		}}
}

// NewSMS sends SMS through an HTTP provider.
func NewSMS(baseURL string) *HTTP {
	return &HTTP{client: newHTTPClient(), url: baseURL + "/sms/send", provider: "mock-sms",
		payload: func(m *pb.DispatchMessage) any {
			return map[string]string{"to": m.Target, "body": m.Body, "notification_id": m.NotificationId}
		}}
}

// Send posts the message and classifies the provider response as success, permanent or transient.
func (h *HTTP) Send(ctx context.Context, msg *pb.DispatchMessage) (Result, error) {
	body, err := json.Marshal(h.payload(msg))
	if err != nil {
		return Result{}, Permanent(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		return Result{}, Permanent(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode/100 == 2:
		var out struct {
			MessageID string `json:"message_id"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return Result{Provider: h.provider, ProviderID: out.MessageID}, nil
	case resp.StatusCode == http.StatusGone || (resp.StatusCode == http.StatusNotFound && msg.Channel == "push"):
		return Result{}, ErrInvalidToken
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return Result{}, fmt.Errorf("provider status %d", resp.StatusCode)
	default:
		return Result{}, Permanent(fmt.Errorf("provider rejected with status %d", resp.StatusCode))
	}
}
