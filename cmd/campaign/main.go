package main

import (
	"fmt"
	"os"
	"time"

	"notification-service/internal/app"
	"notification-service/internal/blob"
	"notification-service/internal/campaign"
	"notification-service/internal/config"
	"notification-service/internal/store/dynamo"
)

type cfg struct {
	config.Base
	config.Dynamo
	config.S3
	Workers int `env:"CAMPAIGN_WORKERS" envDefault:"32"`
}

// main runs the bulk campaign runner.
func main() {
	c, err := config.Load[cfg]()
	if err != nil {
		panic(err)
	}
	c.ServiceName = "campaign"
	if c.AdminAddr == "" {
		c.AdminAddr = ":9109"
	}

	app.Run(c.Base, nil, func(rt app.Runtime) error {
		awsCfg, err := dynamo.LoadAWS(rt.Ctx, c.Dynamo)
		if err != nil {
			return err
		}
		db := dynamo.NewClient(awsCfg, c.Dynamo)
		tables := dynamo.NewTables(c.TablePrefix)
		host, _ := os.Hostname()
		r := &campaign.Runner{
			Store:   dynamo.NewCampaigns(db, tables),
			Source:  blob.New(awsCfg, c.S3.Endpoint, c.S3.Bucket),
			Emitter: dynamo.NewNotifications(db, tables),
			Leases:  dynamo.NewLeases(db, tables, 30*time.Second),
			Owner:   fmt.Sprintf("%s-%d", host, os.Getpid()),
			Log:     rt.Log,
			Poll:    2 * time.Second,
			Workers: c.Workers,
			Now:     time.Now,
		}
		return r.Run(rt.Ctx)
	})
}
