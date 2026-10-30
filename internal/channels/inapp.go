package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"

	"github.com/redis/go-redis/v9"

	pb "notification-service/internal/events/notification/v1"
)

type Inbox interface {
	Put(ctx context.Context, userID, notificationID, title, body string, createdAt int64) error
}

type InApp struct {
	Inbox Inbox
	RDB   *redis.Client
}

const InAppShards = 64

// InAppChannel returns the sharded Redis pub/sub channel carrying live inbox events for a user.
func InAppChannel(userID string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(userID))
	return fmt.Sprintf("inapp:%02d", h.Sum32()%InAppShards)
}

// Send stores the message in the user's inbox and pushes it to any live connection.
func (a *InApp) Send(ctx context.Context, msg *pb.DispatchMessage) (Result, error) {
	if err := a.Inbox.Put(ctx, msg.UserId, msg.NotificationId, msg.Title, msg.Body, msg.CreatedAt); err != nil {
		return Result{}, err
	}
	payload, _ := json.Marshal(map[string]any{
		"user_id": msg.UserId, "notification_id": msg.NotificationId, "title": msg.Title, "body": msg.Body, "created_at": msg.CreatedAt,
	})
	_ = a.RDB.Publish(ctx, InAppChannel(msg.UserId), payload).Err()
	return Result{Provider: "inbox", ProviderID: msg.NotificationId, Delivered: true}, nil
}
