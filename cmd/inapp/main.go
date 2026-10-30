package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"notification-service/internal/app"
	"notification-service/internal/config"
	"notification-service/internal/inapp"
)

type cfg struct {
	config.Base
	config.Redis
	ListenAddr string `env:"INAPP_ADDR" envDefault:":8083"`
	APIKeys    string `env:"API_KEYS" envDefault:"demo:demo-key"`
}

// main runs the in-app WebSocket gateway.
func main() {
	c, err := config.Load[cfg]()
	if err != nil {
		panic(err)
	}
	c.ServiceName = "inapp"
	if c.AdminAddr == "" {
		c.AdminAddr = ":9108"
	}
	keys := map[string]bool{}
	for _, pair := range strings.Split(c.APIKeys, ",") {
		if _, key, ok := strings.Cut(strings.TrimSpace(pair), ":"); ok && key != "" {
			keys[key] = true
		}
	}

	app.Run(c.Base, nil, func(rt app.Runtime) error {
		rdb := redis.NewClient(&redis.Options{Addr: c.Redis.Addr})
		defer rdb.Close()
		hub := inapp.NewHub(rdb, rt.Log)
		go hub.Run(rt.Ctx)

		srv := &http.Server{Addr: c.ListenAddr, Handler: (&inapp.Server{Hub: hub, Keys: keys}).Handler(), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			<-rt.Ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown)
		}()
		rt.Log.Info("listening", "addr", c.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
}
