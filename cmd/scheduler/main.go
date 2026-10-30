package main

import (
	"fmt"
	"os"
	"time"

	"notification-service/internal/app"
	"notification-service/internal/config"
	"notification-service/internal/kafkax"
	"notification-service/internal/scheduler"
	"notification-service/internal/store/dynamo"
)

type cfg struct {
	config.Base
	config.Kafka
	config.Dynamo
	Tick    time.Duration `env:"SCHEDULER_TICK" envDefault:"1s"`
	Catchup time.Duration `env:"SCHEDULER_CATCHUP" envDefault:"24h"`
}

// main runs the delayed-delivery scheduler.
func main() {
	c, err := config.Load[cfg]()
	if err != nil {
		panic(err)
	}
	c.ServiceName = "scheduler"
	if c.AdminAddr == "" {
		c.AdminAddr = ":9106"
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

		db := dynamo.NewClient(awsCfg, c.Dynamo)
		tables := dynamo.NewTables(c.TablePrefix)
		host, _ := os.Hostname()
		s := &scheduler.Scheduler{
			Store:   dynamo.NewSchedule(db, tables),
			Leases:  dynamo.NewLeases(db, tables, 15*time.Second),
			Kafka:   kc,
			Topic:   kafkax.TopicRequested,
			Owner:   fmt.Sprintf("%s-%d", host, os.Getpid()),
			Log:     rt.Log,
			Tick:    c.Tick,
			Catchup: c.Catchup,
			Now:     time.Now,
		}
		return s.Run(rt.Ctx)
	})
}
