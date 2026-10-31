package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"notification-service/internal/api"
	"notification-service/internal/app"
	"notification-service/internal/blob"
	"notification-service/internal/config"
	"notification-service/internal/store/dynamo"
	"notification-service/internal/users"
)

type cfg struct {
	config.Base
	config.Dynamo
	config.Redis
	config.S3
	ListenAddr  string  `env:"API_ADDR" envDefault:":8081"`
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
		db := dynamo.NewClient(awsCfg, c.Dynamo)
		tables := dynamo.NewTables(c.TablePrefix)
		repo = dynamo.NewNotifications(db, tables)
		rdb := redis.NewClient(&redis.Options{Addr: c.Redis.Addr})
		defer rdb.Close()
		userSvc := users.NewService(dynamo.NewUsers(db, tables), rdb, 5*time.Minute)

		srv := &http.Server{
			Addr: c.ListenAddr,
			Handler: api.NewServer(api.Deps{
				Store:     repo,
				Users:     userSvc,
				Templates: dynamo.NewTemplates(db, tables),
				Inbox:     dynamo.NewInbox(db, tables),
				Campaigns: dynamo.NewCampaigns(db, tables),
				Blob:      blob.New(awsCfg, c.S3.Endpoint, c.S3.Bucket),
				Auth:      api.NewAuth(keys, c.ProducerRPS, c.ProducerBst),
			}).Router(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			<-rt.Ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown)
		}()
		rt.Log.Info("listening", "addr", c.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	})
}
