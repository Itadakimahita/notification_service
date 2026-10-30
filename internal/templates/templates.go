package templates

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	htmltemplate "html/template"
	"io"
	"strings"
	"sync"
	"text/template"
	"time"

	"notification-service/internal/domain"
	"notification-service/internal/store/dynamo"
)

var ErrNotFound = errors.New("template not found")

type Source interface {
	GetLatest(ctx context.Context, id string, ch domain.Channel, locale string) (*domain.Template, error)
}

type Compiled struct {
	Version int
	subject *template.Template
	body    renderer
}

type renderer interface {
	Execute(w io.Writer, data any) error
}

type textRenderer struct{ t *template.Template }

func (r textRenderer) Execute(w io.Writer, data any) error {
	return r.t.Execute(w, data)
}

type htmlRenderer struct{ t *htmltemplate.Template }

func (r htmlRenderer) Execute(w io.Writer, data any) error {
	return r.t.Execute(w, data)
}

// Compile parses a template, using HTML escaping for email bodies.
func Compile(t domain.Template) (*Compiled, error) {
	c := &Compiled{Version: t.Version}
	var err error
	if c.subject, err = template.New("subject").Option("missingkey=error").Parse(t.Subject); err != nil {
		return nil, fmt.Errorf("subject: %w", err)
	}
	if t.Channel == domain.ChannelEmail {
		ht, err := htmltemplate.New("body").Option("missingkey=error").Parse(t.Body)
		if err != nil {
			return nil, fmt.Errorf("body: %w", err)
		}
		c.body = htmlRenderer{ht}
		return c, nil
	}
	tt, err := template.New("body").Option("missingkey=error").Parse(t.Body)
	if err != nil {
		return nil, fmt.Errorf("body: %w", err)
	}
	c.body = textRenderer{tt}
	return c, nil
}

// Render fills the template with data and returns subject and body.
func (c *Compiled) Render(data map[string]string) (string, string, error) {
	var subj, body bytes.Buffer
	if err := c.subject.Execute(&subj, data); err != nil {
		return "", "", err
	}
	if err := c.body.Execute(&body, data); err != nil {
		return "", "", err
	}
	return subj.String(), body.String(), nil
}

// LocaleChain returns the lookup order for a locale, ending in the default.
func LocaleChain(locale string) []string {
	chain := []string{}
	add := func(l string) {
		for _, e := range chain {
			if e == l {
				return
			}
		}
		chain = append(chain, l)
	}
	if locale != "" {
		add(locale)
		if lang, _, ok := strings.Cut(locale, "-"); ok {
			add(lang)
		}
	}
	add(domain.DefaultLocale)
	return chain
}

type entry struct {
	c       *Compiled
	err     error
	expires time.Time
}

type Service struct {
	src   Source
	ttl   time.Duration
	mu    sync.RWMutex
	cache map[string]entry
	now   func() time.Time
}

// NewService resolves templates with locale fallback and caches compiled results.
func NewService(src Source, ttl time.Duration) *Service {
	return &Service{src: src, ttl: ttl, cache: map[string]entry{}, now: time.Now}
}

// Resolve finds the first available locale in the chain and returns the compiled template.
func (s *Service) Resolve(ctx context.Context, id string, ch domain.Channel, locales []string) (*Compiled, error) {
	key := id + "|" + string(ch) + "|" + strings.Join(locales, ",")
	s.mu.RLock()
	e, ok := s.cache[key]
	s.mu.RUnlock()
	if ok && s.now().Before(e.expires) {
		return e.c, e.err
	}

	var found *domain.Template
	for _, loc := range locales {
		t, err := s.src.GetLatest(ctx, id, ch, loc)
		if err == nil {
			found = t
			break
		}
		if !errors.Is(err, dynamo.ErrNotFound) {
			return nil, err
		}
	}
	e = entry{expires: s.now().Add(s.ttl)}
	if found == nil {
		e.err = ErrNotFound
	} else if e.c, e.err = Compile(*found); e.err != nil {
		e.err = fmt.Errorf("compile: %w", e.err)
	}
	s.mu.Lock()
	s.cache[key] = e
	s.mu.Unlock()
	return e.c, e.err
}
