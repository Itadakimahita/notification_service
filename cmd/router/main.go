package main

import (
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"notification-service/internal/app"
	"notification-service/internal/config"
	"notification-service/internal/kafkax"
	"notification-service/internal/router"
	"notification-service/internal/store/dynamo"
	"notification-service/internal/templates"
	"notification-service/internal/users"
)

type cfg struct {
	config.Base
	config.Kafka
	config.Dynamo
	config.Redis
	MarketingCap int64 `env:"MARKETING_DAILY_CAP" envDefault:"3"`
	Parallelism  int   `env:"ROUTER_PARALLELISM" envDefault:"64"`
	BatchSize    int   `env:"ROUTER_BATCH_SIZE" envDefault:"500"`
}

// main runs the policy and fan-out router.
func main() {
	c, err := config.Load[cfg]()
	if err != nil {
		panic(err)
	}
	c.ServiceName = "router"
	if c.AdminAddr == "" {
		c.AdminAddr = ":9103"
	}

	app.Run(c.Base, nil, func(rt app.Runtime) error {
		awsCfg, err := dynamo.LoadAWS(rt.Ctx, c.Dynamo)
		if err != nil {
			return err
		}
		db := dynamo.NewClient(awsCfg, c.Dynamo)
		tables := dynamo.NewTables(c.TablePrefix)
		rdb := redis.NewClient(&redis.Options{Addr: c.Addr})
		defer rdb.Close()

		kc, err := kafkax.NewClient(c.Brokers,
			kgo.ConsumerGroup("router"),
			kgo.ConsumeTopics(kafkax.TopicRequested),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			return fmt.Errorf("kafka: %w", err)
		}
		defer kafkax.Close(kc, 10*time.Second)

		consumer := &router.Consumer{
			Router: &router.Router{
				Users:     users.NewService(dynamo.NewUsers(db, tables), rdb, 5*time.Minute),
				Templates: templates.NewService(dynamo.NewTemplates(db, tables), time.Minute),
				Caps:      &router.RedisCaps{RDB: rdb, Marketing: c.MarketingCap},
				Now:       time.Now,
			},
			Client:      kc,
			Schedule:    dynamo.NewSchedule(db, tables),
			Log:         rt.Log,
			Parallelism: c.Parallelism,
			BatchSize:   c.BatchSize,
		}
		return consumer.Run(rt.Ctx)
	})
}
