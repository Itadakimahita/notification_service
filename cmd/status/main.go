package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"notification-service/internal/app"
	"notification-service/internal/config"
	"notification-service/internal/kafkax"
	"notification-service/internal/status"
	"notification-service/internal/store/dynamo"
)

type cfg struct {
	config.Base
	config.Kafka
	config.Dynamo
	Addr        string `env:"STATUS_ADDR" envDefault:":8082"`
	WebhookKeys string `env:"WEBHOOK_KEYS" envDefault:"mock:mock-webhook-key"`
	BatchSize   int    `env:"STATUS_BATCH_SIZE" envDefault:"1000"`
	Parallelism int    `env:"STATUS_PARALLELISM" envDefault:"32"`
}

// main runs the status updater and the provider webhook receiver.
func main() {
	c, err := config.Load[cfg]()
	if err != nil {
		panic(err)
	}
	c.ServiceName = "status"
	if c.AdminAddr == "" {
		c.AdminAddr = ":9107"
	}
	keys := map[string]string{}
	for _, pair := range strings.Split(c.WebhookKeys, ",") {
		provider, key, ok := strings.Cut(strings.TrimSpace(pair), ":")
		if !ok || provider == "" || key == "" {
			panic("WEBHOOK_KEYS must be provider:key pairs")
		}
		keys[provider] = key
	}

	app.Run(c.Base, nil, func(rt app.Runtime) error {
		awsCfg, err := dynamo.LoadAWS(rt.Ctx, c.Dynamo)
		if err != nil {
			return err
		}
		db := dynamo.NewClient(awsCfg, c.Dynamo)

		kc, err := kafkax.NewClient(c.Brokers,
			kgo.ConsumerGroup("status"),
			kgo.ConsumeTopics(kafkax.TopicStatus),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			return err
		}
		defer kafkax.Close(kc, 10*time.Second)

		srv := &http.Server{
			Addr:              c.Addr,
			Handler:           (&status.Webhook{Kafka: kc, Keys: keys, Now: time.Now}).Router(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			<-rt.Ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown)
		}()
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				rt.Log.Error("webhook server failed", "err", err)
			}
		}()

		consumer := &status.Consumer{
			Store: dynamo.NewStatuses(db, dynamo.NewTables(c.TablePrefix)), Client: kc, Log: rt.Log,
			BatchSize: c.BatchSize, Parallelism: c.Parallelism,
		}
		return consumer.Run(rt.Ctx)
	})
}
