package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"notification-service/internal/app"
	"notification-service/internal/config"
)

type cfg struct {
	config.Base
	Addr       string        `env:"MOCK_ADDR" envDefault:":9200"`
	ErrorRate  float64       `env:"MOCK_ERROR_RATE" envDefault:"0"`
	Latency    time.Duration `env:"MOCK_LATENCY" envDefault:"50ms"`
	WebhookURL string        `env:"MOCK_WEBHOOK_URL"`
	WebhookKey string        `env:"WEBHOOK_KEY" envDefault:"mock-webhook-key"`
}

type settings struct {
	ErrorRate float64 `json:"error_rate"`
	LatencyMS int64   `json:"latency_ms"`
}

type server struct {
	mu      sync.RWMutex
	s       settings
	webhook string
	key     string
	client  *http.Client
}

var requests = promauto.NewCounterVec(prometheus.CounterOpts{Name: "mockprovider_requests_total", Help: "Mock provider requests."}, []string{"kind", "code"})

func (s *server) snapshot() settings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.s
}

// send simulates a provider: latency, random 429/503 failures, invalid and rejected tokens, and delivery callbacks.
func (s *server) send(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req map[string]string
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respond(w, kind, http.StatusBadRequest, nil)
			return
		}
		cfg := s.snapshot()
		if cfg.LatencyMS > 0 {
			d := time.Duration(float64(cfg.LatencyMS) * (0.8 + 0.4*rand.Float64()) * float64(time.Millisecond))
			select {
			case <-r.Context().Done():
				return
			case <-time.After(d):
			}
		}
		target := req["token"] + req["to"]
		switch {
		case strings.HasPrefix(target, "invalid-"):
			respond(w, kind, http.StatusGone, nil)
		case strings.HasPrefix(target, "bad-"):
			respond(w, kind, http.StatusBadRequest, nil)
		case rand.Float64() < cfg.ErrorRate:
			code := http.StatusServiceUnavailable
			if rand.Float64() < 0.3 {
				code = http.StatusTooManyRequests
			}
			respond(w, kind, code, nil)
		default:
			id := uuid.NewString()
			respond(w, kind, http.StatusOK, map[string]string{"message_id": id})
			if s.webhook != "" {
				go s.deliver(kind, req["notification_id"], id)
			}
		}
	}
}

func respond(w http.ResponseWriter, kind string, code int, body any) {
	requests.WithLabelValues(kind, http.StatusText(code)).Inc()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if body != nil {
		_ = json.NewEncoder(w).Encode(body)
	}
}

// deliver posts the delivery callback, retrying with backoff like a real provider would.
func (s *server) deliver(kind, notificationID, providerID string) {
	time.Sleep(time.Duration(50+rand.IntN(250)) * time.Millisecond)
	body, _ := json.Marshal(map[string]string{
		"notification_id": notificationID, "channel": kind, "provider_message_id": providerID, "status": "delivered",
	})
	for attempt := 0; attempt < 6; attempt++ {
		req, err := http.NewRequest(http.MethodPost, s.webhook, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Webhook-Key", s.key)
		if resp, err := s.client.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				return
			}
		}
		time.Sleep(time.Duration(200<<attempt) * time.Millisecond)
	}
}

func (s *server) config(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var in settings
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.ErrorRate < 0 || in.ErrorRate > 1 || in.LatencyMS < 0 {
			http.Error(w, "invalid settings", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.s = in
		s.mu.Unlock()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.snapshot())
}

// main runs the fake push/SMS provider used for local runs and chaos tests.
func main() {
	c, err := config.Load[cfg]()
	if err != nil {
		panic(err)
	}
	c.ServiceName = "mockprovider"
	if c.AdminAddr == "" {
		c.AdminAddr = ":9105"
	}
	app.Run(c.Base, nil, func(rt app.Runtime) error {
		s := &server{
			s:       settings{ErrorRate: c.ErrorRate, LatencyMS: c.Latency.Milliseconds()},
			webhook: c.WebhookURL,
			key:     c.WebhookKey,
			client:  &http.Client{Timeout: 3 * time.Second},
		}
		mux := http.NewServeMux()
		mux.HandleFunc("POST /push/send", s.send("push"))
		mux.HandleFunc("POST /sms/send", s.send("sms"))
		mux.HandleFunc("/admin/config", s.config)
		srv := &http.Server{Addr: c.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			<-rt.Ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown)
		}()
		rt.Log.Info("mock provider listening", "addr", c.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
}
