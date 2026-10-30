package router

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"notification-service/internal/domain"
	pb "notification-service/internal/events/notification/v1"
	"notification-service/internal/templates"
)

const allChannels = "all"

var defaultChannels = []domain.Channel{domain.ChannelPush, domain.ChannelInApp, domain.ChannelEmail}

type Users interface {
	Load(ctx context.Context, userID string) (domain.UserData, error)
}

type Templates interface {
	Resolve(ctx context.Context, id string, ch domain.Channel, locales []string) (*templates.Compiled, error)
}

type Caps interface {
	Allow(ctx context.Context, userID string, cat domain.Category, notificationID string, now time.Time) (bool, error)
}

type Deferral struct {
	Channels []string
	At       time.Time
}

type Outcome struct {
	Dispatch []*pb.DispatchMessage
	Status   []*pb.StatusEvent
	Deferred []Deferral
}

type Router struct {
	Users     Users
	Templates Templates
	Caps      Caps
	Now       func() time.Time
}

type target struct {
	value    string
	platform string
}

// Route applies expiry, scheduling, preference, quiet-hour and cap policy and fans a request out to channel messages.
func (r *Router) Route(ctx context.Context, req *pb.NotificationRequested) (Outcome, error) {
	now := r.Now()
	var out Outcome
	status := func(ch string, st domain.Status, detail string) {
		out.Status = append(out.Status, &pb.StatusEvent{
			NotificationId: req.NotificationId, Channel: ch, Status: string(st), Detail: detail, At: now.Unix(),
		})
	}

	if req.ExpiresAt > 0 && now.Unix() >= req.ExpiresAt {
		status(allChannels, domain.StatusExpired, "expired before routing")
		return out, nil
	}
	if !req.Released && req.SendAt > now.Unix() {
		out.Deferred = append(out.Deferred, Deferral{Channels: req.Channels, At: time.Unix(req.SendAt, 0)})
		status(allChannels, domain.StatusScheduled, "send_at")
		return out, nil
	}

	user, err := r.Users.Load(ctx, req.UserId)
	if err != nil {
		return out, fmt.Errorf("load user: %w", err)
	}
	if !user.Found {
		status(allChannels, domain.StatusSuppressed, "unknown user")
		return out, nil
	}

	cat := domain.Category(req.Category)
	loc := user.Profile.Location()
	locales := templates.LocaleChain(user.Profile.Locale)

	type ready struct {
		ch      domain.Channel
		targets []target
	}
	var pending []ready
	for _, ch := range requestedChannels(req) {
		pref, has := user.Pref(cat, ch)
		if has && !pref.Enabled {
			status(string(ch), domain.StatusSuppressed, "opted out")
			continue
		}
		if cat == domain.CategoryMarketing && has {
			if quiet, until := quietUntil(now, loc, pref.QuietStart, pref.QuietEnd); quiet {
				out.Deferred = append(out.Deferred, Deferral{Channels: []string{string(ch)}, At: until})
				status(string(ch), domain.StatusScheduled, "quiet hours")
				continue
			}
		}
		targets, reason := resolveTargets(user, ch)
		if len(targets) == 0 {
			status(string(ch), domain.StatusSuppressed, reason)
			continue
		}
		pending = append(pending, ready{ch, targets})
	}

	if len(pending) > 0 && r.Caps != nil {
		ok, err := r.Caps.Allow(ctx, req.UserId, cat, req.NotificationId, now)
		if err != nil {
			return out, fmt.Errorf("frequency cap: %w", err)
		}
		if !ok {
			for _, p := range pending {
				status(string(p.ch), domain.StatusSuppressed, "frequency cap")
			}
			return out, nil
		}
	}

	routed := false
	for _, p := range pending {
		tpl, err := r.Templates.Resolve(ctx, req.TemplateId, p.ch, locales)
		if errors.Is(err, templates.ErrNotFound) {
			status(string(p.ch), domain.StatusFailed, "template not found")
			continue
		}
		if err != nil {
			return out, fmt.Errorf("resolve template: %w", err)
		}
		title, body, err := tpl.Render(req.Data)
		if err != nil {
			status(string(p.ch), domain.StatusFailed, "template render: "+err.Error())
			continue
		}
		for _, t := range p.targets {
			out.Dispatch = append(out.Dispatch, &pb.DispatchMessage{
				NotificationId: req.NotificationId, Producer: req.Producer, UserId: req.UserId,
				Channel: string(p.ch), Target: t.value, Platform: t.platform, Title: title, Body: body,
				Category: req.Category, Priority: req.Priority, ExpiresAt: req.ExpiresAt, CreatedAt: req.CreatedAt,
			})
		}
		status(string(p.ch), domain.StatusQueued, fmt.Sprintf("%d target(s)", len(p.targets)))
		routed = true
	}
	if routed {
		status(allChannels, domain.StatusRouted, "")
	}
	return out, nil
}

func requestedChannels(req *pb.NotificationRequested) []domain.Channel {
	if len(req.Channels) == 0 {
		return defaultChannels
	}
	var out []domain.Channel
	for _, c := range req.Channels {
		if ch := domain.Channel(c); ch.Valid() {
			out = append(out, ch)
		}
	}
	return out
}

func resolveTargets(u domain.UserData, ch domain.Channel) ([]target, string) {
	switch ch {
	case domain.ChannelPush:
		var out []target
		for _, d := range u.ActiveDevices() {
			out = append(out, target{value: d.Token, platform: d.Platform})
		}
		return out, "no active device"
	case domain.ChannelEmail:
		if u.Profile.Email == "" {
			return nil, "no email address"
		}
		return []target{{value: u.Profile.Email}}, ""
	case domain.ChannelSMS:
		if u.Profile.Phone == "" {
			return nil, "no phone number"
		}
		return []target{{value: u.Profile.Phone}}, ""
	default:
		return []target{{value: u.Profile.UserID}}, ""
	}
}

type RedisCaps struct {
	RDB       *redis.Client
	Marketing int64
}

// Allow limits marketing notifications per user per UTC day; redelivery of a counted notification is allowed.
func (c *RedisCaps) Allow(ctx context.Context, userID string, cat domain.Category, nid string, now time.Time) (bool, error) {
	if cat != domain.CategoryMarketing || c.Marketing <= 0 {
		return true, nil
	}
	key := fmt.Sprintf("cap:%s:%s:%s", userID, cat, now.UTC().Format("20060102"))
	added, err := c.RDB.SAdd(ctx, key, nid).Result()
	if err != nil {
		return false, err
	}
	if added == 0 {
		return true, nil
	}
	c.RDB.Expire(ctx, key, 48*time.Hour)
	n, err := c.RDB.SCard(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if n > c.Marketing {
		c.RDB.SRem(ctx, key, nid)
		return false, nil
	}
	return true, nil
}
