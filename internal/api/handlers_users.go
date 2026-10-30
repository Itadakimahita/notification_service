package api

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"notification-service/internal/domain"
	"notification-service/internal/httpx"
	"notification-service/internal/router"
	"notification-service/internal/templates"
)

type Users interface {
	PutProfile(ctx context.Context, p domain.Profile) error
	PutPreferences(ctx context.Context, userID string, prefs []domain.Preference) error
	PutDevice(ctx context.Context, userID string, d domain.Device) (string, error)
	DeleteDevice(ctx context.Context, userID, deviceID string) error
}

type Templates interface {
	Put(ctx context.Context, t domain.Template) (domain.Template, error)
}

type profileRequest struct {
	Locale   string `json:"locale"`
	Timezone string `json:"timezone"`
	Email    string `json:"email"`
	Phone    string `json:"phone"`
}

// putProfile creates or replaces a user profile.
func (s *Server) putProfile(w http.ResponseWriter, r *http.Request) {
	var req profileRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Timezone != "" {
		if _, err := time.LoadLocation(req.Timezone); err != nil {
			httpx.Error(w, http.StatusBadRequest, "unknown timezone")
			return
		}
	}
	p := domain.Profile{UserID: chi.URLParam(r, "id"), Locale: req.Locale, Timezone: req.Timezone, Email: req.Email, Phone: req.Phone}
	if err := s.users.PutProfile(r.Context(), p); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to save profile")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type preferencesRequest struct {
	Preferences []domain.Preference `json:"preferences"`
}

// putPreferences upserts per category and channel preferences.
func (s *Server) putPreferences(w http.ResponseWriter, r *http.Request) {
	var req preferencesRequest
	if !decode(w, r, &req) {
		return
	}
	if len(req.Preferences) == 0 || len(req.Preferences) > 32 {
		httpx.Error(w, http.StatusBadRequest, "preferences must contain 1..32 entries")
		return
	}
	for _, p := range req.Preferences {
		if !p.Category.Valid() || !p.Channel.Valid() {
			httpx.Error(w, http.StatusBadRequest, "invalid category or channel")
			return
		}
		if (p.QuietStart != "" || p.QuietEnd != "") && !(router.ValidClock(p.QuietStart) && router.ValidClock(p.QuietEnd)) {
			httpx.Error(w, http.StatusBadRequest, "quiet hours must be HH:MM")
			return
		}
	}
	if err := s.users.PutPreferences(r.Context(), chi.URLParam(r, "id"), req.Preferences); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to save preferences")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type deviceRequest struct {
	Token    string `json:"token"`
	Platform string `json:"platform"`
}

// addDevice registers a push token for a user.
func (s *Server) addDevice(w http.ResponseWriter, r *http.Request) {
	var req deviceRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Token == "" || len(req.Token) > 4096 {
		httpx.Error(w, http.StatusBadRequest, "token is required")
		return
	}
	if req.Platform != "ios" && req.Platform != "android" && req.Platform != "web" {
		httpx.Error(w, http.StatusBadRequest, "platform must be ios, android or web")
		return
	}
	id, err := s.users.PutDevice(r.Context(), chi.URLParam(r, "id"), domain.Device{Token: req.Token, Platform: req.Platform})
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to save device")
		return
	}
	httpx.JSON(w, http.StatusCreated, map[string]string{"device_id": id})
}

// removeDevice deletes a registered device.
func (s *Server) removeDevice(w http.ResponseWriter, r *http.Request) {
	if err := s.users.DeleteDevice(r.Context(), chi.URLParam(r, "id"), chi.URLParam(r, "device_id")); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to delete device")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type templateRequest struct {
	TemplateID string         `json:"template_id"`
	Channel    domain.Channel `json:"channel"`
	Locale     string         `json:"locale"`
	Subject    string         `json:"subject"`
	Body       string         `json:"body"`
}

// putTemplate validates and stores a new template version.
func (s *Server) putTemplate(w http.ResponseWriter, r *http.Request) {
	var req templateRequest
	if !decode(w, r, &req) {
		return
	}
	if req.TemplateID == "" || !req.Channel.Valid() || req.Body == "" {
		httpx.Error(w, http.StatusBadRequest, "template_id, channel and body are required")
		return
	}
	if req.Locale == "" {
		req.Locale = domain.DefaultLocale
	}
	t := domain.Template{ID: req.TemplateID, Channel: req.Channel, Locale: req.Locale, Subject: req.Subject, Body: req.Body}
	if _, err := templates.Compile(t); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid template: "+err.Error())
		return
	}
	saved, err := s.templates.Put(r.Context(), t)
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "failed to save template")
		return
	}
	httpx.JSON(w, http.StatusCreated, saved)
}
