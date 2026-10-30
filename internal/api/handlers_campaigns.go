package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"notification-service/internal/domain"
	"notification-service/internal/httpx"
	"notification-service/internal/store/dynamo"
)

type Campaigns interface {
	Create(ctx context.Context, c *dynamo.Campaign) error
	Get(ctx context.Context, id string) (*dynamo.Campaign, error)
	SetStatus(ctx context.Context, id string, from []dynamo.CampaignStatus, to dynamo.CampaignStatus, errMsg string) error
}

type Presigner interface {
	PresignPut(ctx context.Context, key string, ttl time.Duration) (string, error)
}

type campaignRequest struct {
	Name       string            `json:"name"`
	TemplateID string            `json:"template_id"`
	Category   domain.Category   `json:"category"`
	Channels   []domain.Channel  `json:"channels"`
	Data       map[string]string `json:"data"`
	RatePerSec int               `json:"rate_per_sec"`
}

// createCampaign registers a draft campaign and returns a presigned URL for uploading the audience CSV.
func (s *Server) createCampaign(w http.ResponseWriter, r *http.Request) {
	var req campaignRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Name == "" || len(req.Name) > 200 || req.TemplateID == "" {
		httpx.Error(w, http.StatusBadRequest, "name and template_id are required")
		return
	}
	if req.Category == "" {
		req.Category = domain.CategoryMarketing
	}
	if !req.Category.Valid() {
		httpx.Error(w, http.StatusBadRequest, "category must be transactional or marketing")
		return
	}
	if req.RatePerSec == 0 {
		req.RatePerSec = 1000
	}
	if req.RatePerSec < 1 || req.RatePerSec > 50000 {
		httpx.Error(w, http.StatusBadRequest, "rate_per_sec must be 1..50000")
		return
	}
	channels := make([]string, 0, len(req.Channels))
	for _, ch := range req.Channels {
		if !ch.Valid() {
			httpx.Error(w, http.StatusBadRequest, "unknown channel "+string(ch))
			return
		}
		channels = append(channels, string(ch))
	}

	id := uuid.Must(uuid.NewV7()).String()
	c := &dynamo.Campaign{
		ID: id, Name: req.Name, TemplateID: req.TemplateID, Category: string(req.Category), Channels: channels,
		Data: req.Data, RatePerSec: req.RatePerSec, AudienceKey: "campaigns/" + id + "/audience.csv", Status: dynamo.CampaignDraft,
	}
	if err := s.campaigns.Create(r.Context(), c); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to create campaign")
		return
	}
	url, err := s.blob.PresignPut(r.Context(), c.AudienceKey, time.Hour)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to presign upload")
		return
	}
	httpx.JSON(w, http.StatusCreated, map[string]any{"campaign_id": id, "status": c.Status, "audience_key": c.AudienceKey, "upload_url": url})
}

// getCampaign returns campaign status and progress.
func (s *Server) getCampaign(w http.ResponseWriter, r *http.Request) {
	c, err := s.campaigns.Get(r.Context(), chi.URLParam(r, "id"))
	switch {
	case errors.Is(err, dynamo.ErrNotFound):
		httpx.Error(w, http.StatusNotFound, "campaign not found")
	case err != nil:
		httpx.Error(w, http.StatusInternalServerError, "failed to load campaign")
	default:
		httpx.JSON(w, http.StatusOK, c)
	}
}

var transitions = map[string]struct {
	from []dynamo.CampaignStatus
	to   dynamo.CampaignStatus
}{
	"start":  {[]dynamo.CampaignStatus{dynamo.CampaignDraft}, dynamo.CampaignRunning},
	"pause":  {[]dynamo.CampaignStatus{dynamo.CampaignRunning}, dynamo.CampaignPaused},
	"resume": {[]dynamo.CampaignStatus{dynamo.CampaignPaused}, dynamo.CampaignRunning},
	"cancel": {[]dynamo.CampaignStatus{dynamo.CampaignDraft, dynamo.CampaignRunning, dynamo.CampaignPaused}, dynamo.CampaignCancelled},
}

// transition applies start, pause, resume or cancel to a campaign.
func (s *Server) transition(action string) http.HandlerFunc {
	t := transitions[action]
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if _, err := s.campaigns.Get(r.Context(), id); errors.Is(err, dynamo.ErrNotFound) {
			httpx.Error(w, http.StatusNotFound, "campaign not found")
			return
		}
		err := s.campaigns.SetStatus(r.Context(), id, t.from, t.to, "")
		switch {
		case errors.Is(err, dynamo.ErrBadTransition):
			httpx.Error(w, http.StatusConflict, "campaign cannot "+action+" from its current status")
		case err != nil:
			httpx.Error(w, http.StatusInternalServerError, "failed to update campaign")
		default:
			httpx.JSON(w, http.StatusOK, map[string]any{"campaign_id": id, "status": t.to})
		}
	}
}
