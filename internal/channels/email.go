package channels

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/smtp"
	"net/textproto"
	"strings"
	"time"

	pb "notification-service/internal/events/notification/v1"
)

type Email struct {
	Addr    string
	From    string
	Timeout time.Duration
}

// Send delivers an HTML email over SMTP; 5xx replies are permanent, everything else is retryable.
func (e *Email) Send(ctx context.Context, msg *pb.DispatchMessage) (Result, error) {
	host, _, err := net.SplitHostPort(e.Addr)
	if err != nil {
		return Result{}, Permanent(err)
	}
	dialer := net.Dialer{Timeout: e.Timeout}
	conn, err := dialer.DialContext(ctx, "tcp", e.Addr)
	if err != nil {
		return Result{}, err
	}
	_ = conn.SetDeadline(time.Now().Add(e.Timeout))
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		conn.Close()
		return Result{}, err
	}
	defer c.Close()

	if err := c.Mail(e.From); err != nil {
		return Result{}, classifySMTP(err)
	}
	if err := c.Rcpt(msg.Target); err != nil {
		return Result{}, classifySMTP(err)
	}
	w, err := c.Data()
	if err != nil {
		return Result{}, classifySMTP(err)
	}
	headers := []string{
		"From: " + e.From,
		"To: " + msg.Target,
		"Subject: " + mime.QEncoding.Encode("utf-8", msg.Title),
		"Message-ID: <" + msg.NotificationId + "@notifications.local>",
		"MIME-Version: 1.0",
		"Content-Type: text/html; charset=utf-8",
	}
	if _, err := fmt.Fprintf(w, "%s\r\n\r\n%s", strings.Join(headers, "\r\n"), msg.Body); err != nil {
		return Result{}, err
	}
	if err := w.Close(); err != nil {
		return Result{}, classifySMTP(err)
	}
	_ = c.Quit()
	return Result{Provider: "smtp", ProviderID: msg.NotificationId}, nil
}

func classifySMTP(err error) error {
	var tpe *textproto.Error
	if errors.As(err, &tpe) && tpe.Code >= 500 {
		return Permanent(err)
	}
	return err
}
