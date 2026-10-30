package templates

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"notification-service/internal/domain"
	"notification-service/internal/store/dynamo"
)

func TestLocaleChain(t *testing.T) {
	require.Equal(t, []string{"kk-KZ", "kk", "en"}, LocaleChain("kk-KZ"))
	require.Equal(t, []string{"ru", "en"}, LocaleChain("ru"))
	require.Equal(t, []string{"en"}, LocaleChain("en"))
	require.Equal(t, []string{"en"}, LocaleChain(""))
}

func TestRenderEscapesEmailHTMLButNotPush(t *testing.T) {
	email, err := Compile(domain.Template{Channel: domain.ChannelEmail, Subject: "s", Body: "<p>{{.x}}</p>"})
	require.NoError(t, err)
	_, body, err := email.Render(map[string]string{"x": "<b>"})
	require.NoError(t, err)
	require.Equal(t, "<p>&lt;b&gt;</p>", body)

	push, err := Compile(domain.Template{Channel: domain.ChannelPush, Body: "{{.x}}"})
	require.NoError(t, err)
	_, body, err = push.Render(map[string]string{"x": "<b>"})
	require.NoError(t, err)
	require.Equal(t, "<b>", body)
}

func TestCompileRejectsBadSyntax(t *testing.T) {
	_, err := Compile(domain.Template{Channel: domain.ChannelSMS, Body: "{{.x"})
	require.Error(t, err)
}

type countingSource struct{ calls int }

func (c *countingSource) GetLatest(context.Context, string, domain.Channel, string) (*domain.Template, error) {
	c.calls++
	return nil, dynamo.ErrNotFound
}

func TestResolveCachesNegativeResults(t *testing.T) {
	src := &countingSource{}
	s := NewService(src, time.Minute)
	for range 3 {
		_, err := s.Resolve(context.Background(), "x", domain.ChannelPush, []string{"kk", "en"})
		require.True(t, errors.Is(err, ErrNotFound))
	}
	require.Equal(t, 2, src.calls)
}
