package main

import (
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"notification-service/internal/app"
	"notification-service/internal/config"
	"notification-service/internal/kafkax"
	"notification-service/internal/lag"
)

type cfg struct {
	config.Base
	config.Kafka
	Every time.Duration `env:"LAG_INTERVAL" envDefault:"5s"`
}

// main exports consumer lag and dead-letter depth as Prometheus metrics.
func main() {
	c, err := config.Load[cfg]()
	if err != nil {
		panic(err)
	}
	c.ServiceName = "lagexporter"
	if c.AdminAddr == "" {
		c.AdminAddr = ":9110"
	}
	app.Run(c.Base, nil, func(rt app.Runtime) error {
		kc, err := kafkax.NewClient(c.Brokers)
		if err != nil {
			return err
		}
		defer kc.Close()
		return (&lag.Exporter{Adm: kadm.NewClient(kc), Log: rt.Log, Every: c.Every}).Run(rt.Ctx)
	})
}
