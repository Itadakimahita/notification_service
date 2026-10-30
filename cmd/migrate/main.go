package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"notification-service/internal/app"
	"notification-service/internal/config"
	"notification-service/internal/kafkax"
	"notification-service/internal/store/dynamo"
)

type cfg struct {
	config.Base
	config.Kafka
	config.Dynamo
	Replication int16 `env:"KAFKA_REPLICATION" envDefault:"3"`
}

// main creates DynamoDB tables and Kafka topics idempotently.
func main() {
	c, err := config.Load[cfg]()
	if err != nil {
		panic(err)
	}
	c.ServiceName = "migrate"
	c.AdminAddr = ""

	app.Run(c.Base, nil, func(rt app.Runtime) error {
		awsCfg, err := dynamo.LoadAWS(rt.Ctx, c.Dynamo)
		if err != nil {
			return err
		}
		db := dynamo.NewClient(awsCfg, c.Dynamo)
		if err := retry(rt.Ctx, func() error { return dynamo.EnsureTables(rt.Ctx, db, dynamo.NewTables(c.TablePrefix)) }); err != nil {
			return fmt.Errorf("dynamo: %w", err)
		}
		rt.Log.Info("dynamodb tables ready")

		kc, err := kafkax.NewClient(c.Brokers)
		if err != nil {
			return err
		}
		defer kc.Close()
		adm := kadm.NewClient(kc)
		specs := kafkax.AllTopics()
		if err := retry(rt.Ctx, func() error { return kafkax.EnsureTopics(rt.Ctx, adm, specs, c.Replication) }); err != nil {
			return fmt.Errorf("kafka: %w", err)
		}
		rt.Log.Info("kafka topics ready", "count", len(specs))
		return nil
	})
}

// retry runs fn with linear backoff while dependencies are starting up.
func retry(ctx context.Context, fn func() error) error {
	var err error
	for i := 1; i <= 15; i++ {
		if err = fn(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(i) * time.Second):
		}
	}
	return err
}
