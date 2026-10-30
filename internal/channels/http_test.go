package channels

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	pb "notification-service/internal/events/notification/v1"
)

func TestHTTPClassifiesProviderResponses(t *testing.T) {
	cases := []struct {
		code      int
		ok        bool
		permanent bool
		invalid   bool
	}{
		{200, true, false, false},
		{503, false, false, false},
		{429, false, false, false},
		{500, false, false, false},
		{410, false, true, true},
		{404, false, true, true},
		{400, false, true, false},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(c.code)
			if c.code == 200 {
				_, _ = w.Write([]byte(`{"message_id":"m1"}`))
			}
		}))
		res, err := NewPush(srv.URL).Send(context.Background(), &pb.DispatchMessage{Channel: "push", Target: "t"})
		srv.Close()

		if c.ok {
			require.NoError(t, err)
			require.Equal(t, "m1", res.ProviderID)
			continue
		}
		require.Error(t, err, c.code)
		require.Equal(t, c.permanent, IsPermanent(err), "status %d", c.code)
		require.Equal(t, c.invalid, err == ErrInvalidToken, "status %d", c.code)
	}
}

func TestHTTPConnectionErrorIsTransient(t *testing.T) {
	_, err := NewSMS("http://127.0.0.1:1").Send(context.Background(), &pb.DispatchMessage{Channel: "sms", Target: "+7"})
	require.Error(t, err)
	require.False(t, IsPermanent(err))
}
