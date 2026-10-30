package api

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/time/rate"

	"notification-service/internal/httpx"
)

type ctxKey struct{}

type Auth struct {
	keys     map[string]string
	rps      rate.Limit
	burst    int
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
}

// NewAuth maps API keys to producers and applies a per-producer token bucket.
func NewAuth(keys map[string]string, rps float64, burst int) *Auth {
	return &Auth{keys: keys, rps: rate.Limit(rps), burst: burst, limiters: map[string]*rate.Limiter{}}
}

func (a *Auth) limiter(producer string) *rate.Limiter {
	a.mu.Lock()
	defer a.mu.Unlock()
	l, ok := a.limiters[producer]
	if !ok {
		l = rate.NewLimiter(a.rps, a.burst)
		a.limiters[producer] = l
	}
	return l
}

// Middleware authenticates the API key and enforces the producer quota.
func (a *Auth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "ApiKey ")
		producer, known := a.keys[key]
		if !ok || !known {
			httpx.Error(w, http.StatusUnauthorized, "invalid api key")
			return
		}
		if !a.limiter(producer).Allow() {
			w.Header().Set("Retry-After", "1")
			httpx.Error(w, http.StatusTooManyRequests, "producer quota exceeded")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, producer)))
	})
}

// Producer returns the authenticated producer from the request context.
func Producer(ctx context.Context) string {
	p, _ := ctx.Value(ctxKey{}).(string)
	return p
}
