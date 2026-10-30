package inapp

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	"notification-service/internal/httpx"
)

type Server struct {
	Hub  *Hub
	Keys map[string]bool
}

// Handler serves GET /v1/ws?user_id=... for server-side gateways holding an API key.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/ws", s.serve)
	return httpx.MetricsFunc(mux)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "ApiKey ")
	if !ok || !s.Keys[key] {
		httpx.Error(w, http.StatusUnauthorized, "invalid api key")
		return
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" || len(userID) > 128 {
		httpx.Error(w, http.StatusBadRequest, "user_id is required")
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer ws.CloseNow()

	conn := &Conn{UserID: userID, Send: make(chan []byte, 64)}
	if err := s.Hub.Register(r.Context(), conn); err != nil {
		ws.Close(websocket.StatusInternalError, "subscribe failed")
		return
	}
	defer s.Hub.Unregister(conn)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	go func() {
		defer cancel()
		for {
			if _, _, err := ws.Read(ctx); err != nil {
				return
			}
		}
	}()

	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			ws.Close(websocket.StatusNormalClosure, "")
			return
		case msg := <-conn.Send:
			wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
			err := ws.Write(wctx, websocket.MessageText, msg)
			wcancel()
			if err != nil {
				return
			}
		case <-ping.C:
			pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
			err := ws.Ping(pctx)
			pcancel()
			if err != nil {
				return
			}
		}
	}
}
