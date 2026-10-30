package domain

type Channel string

const (
	ChannelPush  Channel = "push"
	ChannelEmail Channel = "email"
	ChannelSMS   Channel = "sms"
	ChannelInApp Channel = "inapp"
)

var Channels = []Channel{ChannelPush, ChannelEmail, ChannelSMS, ChannelInApp}

// Valid reports whether c is a known channel.
func (c Channel) Valid() bool {
	switch c {
	case ChannelPush, ChannelEmail, ChannelSMS, ChannelInApp:
		return true
	}
	return false
}

type Priority string

const (
	PriorityHigh Priority = "high"
	PriorityLow  Priority = "low"
)

var Priorities = []Priority{PriorityHigh, PriorityLow}

// Valid reports whether p is a known priority.
func (p Priority) Valid() bool {
	return p == PriorityHigh || p == PriorityLow
}

type Category string

const (
	CategoryTransactional Category = "transactional"
	CategoryMarketing     Category = "marketing"
)

// Valid reports whether c is a known category.
func (c Category) Valid() bool {
	return c == CategoryTransactional || c == CategoryMarketing
}

type Status string

const (
	StatusAccepted   Status = "ACCEPTED"
	StatusRouted     Status = "ROUTED"
	StatusScheduled  Status = "SCHEDULED"
	StatusQueued     Status = "QUEUED"
	StatusSent       Status = "SENT"
	StatusDelivered  Status = "DELIVERED"
	StatusFailed     Status = "FAILED"
	StatusSuppressed Status = "SUPPRESSED"
	StatusExpired    Status = "EXPIRED"
)
