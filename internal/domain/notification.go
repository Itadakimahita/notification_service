package domain

import "time"

const RetentionDays = 30

type Notification struct {
	ID            string            `dynamodbav:"pk" json:"notification_id"`
	Producer      string            `dynamodbav:"producer" json:"producer"`
	UserID        string            `dynamodbav:"user_id" json:"user_id"`
	TemplateID    string            `dynamodbav:"template_id" json:"template_id"`
	Category      Category          `dynamodbav:"category" json:"category"`
	Priority      Priority          `dynamodbav:"priority" json:"priority"`
	Channels      []Channel         `dynamodbav:"channels,omitempty" json:"channels,omitempty"`
	Data          map[string]string `dynamodbav:"data,omitempty" json:"data,omitempty"`
	SendAt        int64             `dynamodbav:"send_at,omitempty" json:"send_at,omitempty"`
	ExpiresAt     int64             `dynamodbav:"expires_at,omitempty" json:"expires_at,omitempty"`
	CreatedAt     int64             `dynamodbav:"created_at" json:"created_at"`
	Status        Status            `dynamodbav:"status" json:"status"`
	StatusRank    int               `dynamodbav:"status_rank" json:"-"`
	ChannelStatus map[string]Status `dynamodbav:"channel_status" json:"channel_status"`
	ChannelDetail map[string]string `dynamodbav:"channel_detail" json:"channel_detail,omitempty"`
	ChannelRank   map[string]int    `dynamodbav:"channel_rank" json:"-"`
	TTL           int64             `dynamodbav:"ttl" json:"-"`
}

// ComputeTTL sets the retention deadline relative to the delivery start time.
func (n *Notification) ComputeTTL() {
	base := n.CreatedAt
	if n.SendAt > base {
		base = n.SendAt
	}
	n.TTL = time.Unix(base, 0).Add(RetentionDays * 24 * time.Hour).Unix()
}
