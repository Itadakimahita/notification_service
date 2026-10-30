package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"notification-service/internal/api"
	"notification-service/internal/app"
	"notification-service/internal/config"
	"notification-service/internal/store/dynamo"
)

type cfg struct {
	config.Base
	config.Dynamo
	Addr        string  `env:"API_ADDR" envDefault:":8080"`
	APIKeys     string  `env:"API_KEYS" envDefault:"demo:demo-key"`
	ProducerRPS float64 `env:"PRODUCER_RPS" envDefault:"5000"`
	ProducerBst int     `env:"PRODUCER_BURST" envDefault:"10000"`
}

// main runs the public REST API.
func main() {
	c, err := config.Load[cfg]()
	if err != nil {
		panic(err)
	}
	c.ServiceName = "api"
	if c.AdminAddr == "" {
		c.AdminAddr = ":9101"
	}

	keys := map[string]string{}
	for _, pair := range strings.Split(c.APIKeys, ",") {
		producer, key, ok := strings.Cut(strings.TrimSpace(pair), ":")
		if !ok || producer == "" || key == "" {
			panic("API_KEYS must be producer:key pairs")
		}
		keys[key] = producer
	}

	var repo *dynamo.Notifications
	ready := func(ctx context.Context) error { return repo.Ping(ctx) }

	app.Run(c.Base, ready, func(rt app.Runtime) error {
		awsCfg, err := dynamo.LoadAWS(rt.Ctx, c.Dynamo)
		if err != nil {
			return err
		}
		repo = dynamo.NewNotifications(dynamo.NewClient(awsCfg, c.Dynamo), dynamo.NewTables(c.TablePrefix))

		srv := &http.Server{
			Addr:              c.Addr,
			Handler:           api.NewServer(repo, api.NewAuth(keys, c.ProducerRPS, c.ProducerBst)).Router(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			<-rt.Ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown)
		}()
		rt.Log.Info("listening", "addr", c.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	})
}
