package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"notification-service/internal/domain"
	"notification-service/internal/httpx"
	"notification-service/internal/store/dynamo"
)

const (
	maxBodyBytes   = 4 << 20
	maxBatch       = 1000
	batchWorkers   = 32
	idempotencyHdr = "Idempotency-Key"
)

type Store interface {
	Create(ctx context.Context, n *domain.Notification, idemKey string) (string, bool, error)
	Get(ctx context.Context, id string) (*domain.Notification, error)
}

type Server struct {
	store     Store
	users     Users
	templates Templates
	auth      *Auth
	now       func() time.Time
}

// NewServer builds the API server.
func NewServer(store Store, users Users, templates Templates, auth *Auth) *Server {
	return &Server{store: store, users: users, templates: templates, auth: auth, now: time.Now}
}

// Router returns the HTTP handler tree.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(httpx.Metrics)
	r.Group(func(r chi.Router) {
		r.Use(s.auth.Middleware)
		r.Post("/v1/notifications", s.create)
		r.Post("/v1/notifications/batch", s.batch)
		r.Get("/v1/notifications/{id}", s.get)
		r.Put("/v1/users/{id}", s.putProfile)
		r.Put("/v1/users/{id}/preferences", s.putPreferences)
		r.Post("/v1/users/{id}/devices", s.addDevice)
		r.Delete("/v1/users/{id}/devices/{device_id}", s.removeDevice)
		r.Post("/v1/templates", s.putTemplate)
	})
	return r
}

type createResponse struct {
	NotificationID string `json:"notification_id"`
	Duplicate      bool   `json:"duplicate"`
}

func (s *Server) accept(ctx context.Context, req createRequest, idemKey string) (createResponse, int, error) {
	n, err := req.build(Producer(ctx), s.now())
	if err != nil {
		return createResponse{}, http.StatusBadRequest, err
	}
	id, created, err := s.store.Create(ctx, n, idemKey)
	if err != nil {
		return createResponse{}, http.StatusInternalServerError, errors.New("failed to persist notification")
	}
	return createResponse{NotificationID: id, Duplicate: !created}, http.StatusAccepted, nil
}

// create accepts a single notification and returns 202 once it is durably stored.
func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if !decode(w, r, &req) {
		return
	}
	resp, code, err := s.accept(r.Context(), req, r.Header.Get(idempotencyHdr))
	if err != nil {
		httpx.Error(w, code, err.Error())
		return
	}
	httpx.JSON(w, code, resp)
}

type batchRequest struct {
	Items []createRequest `json:"items"`
}

type batchItemResult struct {
	Index          int    `json:"index"`
	NotificationID string `json:"notification_id,omitempty"`
	Duplicate      bool   `json:"duplicate,omitempty"`
	Error          string `json:"error,omitempty"`
}

// batch accepts up to 1000 notifications and reports a result per item.
func (s *Server) batch(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if !decode(w, r, &req) {
		return
	}
	if len(req.Items) == 0 || len(req.Items) > maxBatch {
		httpx.Error(w, http.StatusBadRequest, "items must contain 1..1000 entries")
		return
	}
	results := make([]batchItemResult, len(req.Items))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range min(batchWorkers, len(req.Items)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				item := req.Items[i]
				resp, _, err := s.accept(r.Context(), item, item.IdempotencyKey)
				res := batchItemResult{Index: i}
				if err != nil {
					res.Error = err.Error()
				} else {
					res.NotificationID, res.Duplicate = resp.NotificationID, resp.Duplicate
				}
				results[i] = res
			}
		}()
	}
	for i := range req.Items {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	httpx.JSON(w, http.StatusMultiStatus, map[string]any{"results": results})
}

// get returns the notification and its per-channel delivery status.
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	n, err := s.store.Get(r.Context(), chi.URLParam(r, "id"))
	switch {
	case errors.Is(err, dynamo.ErrNotFound):
		httpx.Error(w, http.StatusNotFound, "notification not found")
	case err != nil:
		httpx.Error(w, http.StatusInternalServerError, "failed to load notification")
	case n.Producer != Producer(r.Context()):
		httpx.Error(w, http.StatusNotFound, "notification not found")
	default:
		httpx.JSON(w, http.StatusOK, n)
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return false
	}
	return true
}
