package main

import (
	"fmt"
	"os"
	"time"

	"notification-service/internal/app"
	"notification-service/internal/config"
	"notification-service/internal/kafkax"
	"notification-service/internal/relay"
	"notification-service/internal/store/dynamo"
)

type cfg struct {
	config.Base
	config.Kafka
	config.Dynamo
}

// main runs the DynamoDB Streams to Kafka outbox relay.
func main() {
	c, err := config.Load[cfg]()
	if err != nil {
		panic(err)
	}
	c.ServiceName = "relay"
	if c.AdminAddr == "" {
		c.AdminAddr = ":9102"
	}

	app.Run(c.Base, nil, func(rt app.Runtime) error {
		awsCfg, err := dynamo.LoadAWS(rt.Ctx, c.Dynamo)
		if err != nil {
			return err
		}
		kc, err := kafkax.NewClient(c.Brokers)
		if err != nil {
			return err
		}
		defer kc.Close()

		host, _ := os.Hostname()
		tables := dynamo.NewTables(c.TablePrefix)
		db := dynamo.NewClient(awsCfg, c.Dynamo)
		r := &relay.Relay{
			DB:        db,
			Streams:   dynamo.NewStreamsClient(awsCfg, c.Dynamo),
			Kafka:     kc,
			Leases:    dynamo.NewLeases(db, tables, 30*time.Second),
			Table:     tables.Notifications,
			Topic:     kafkax.TopicRequested,
			Owner:     fmt.Sprintf("%s-%d", host, os.Getpid()),
			Log:       rt.Log,
			Poll:      200 * time.Millisecond,
			Discovery: 5 * time.Second,
		}
		return r.Run(rt.Ctx)
	})
}
