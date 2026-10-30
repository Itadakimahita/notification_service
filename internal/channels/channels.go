package channels

import (
	"context"
	"errors"

	pb "notification-service/internal/events/notification/v1"
)

var ErrInvalidToken = errors.New("invalid or unregistered token")

type Result struct {
	Provider   string
	ProviderID string
	Delivered  bool
}

type Sender interface {
	Send(ctx context.Context, msg *pb.DispatchMessage) (Result, error)
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks an error that must not be retried.
func Permanent(err error) error { return permanentError{err} }

// IsPermanent reports whether retrying err is pointless.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.Is(err, ErrInvalidToken) || errors.As(err, &p)
}
