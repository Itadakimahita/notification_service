package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"notification-service/internal/app"
	"notification-service/internal/channels"
	"notification-service/internal/config"
	"notification-service/internal/domain"
	"notification-service/internal/kafkax"
	"notification-service/internal/ratelimit"
	"notification-service/internal/store/dynamo"
	"notification-service/internal/users"
	"notification-service/internal/worker"
)

type cfg struct {
	config.Base
	config.Kafka
	config.Dynamo
	config.Redis
	Channel      string        `env:"WORKER_CHANNEL"`
	HighWorkers  int           `env:"HIGH_PARALLELISM" envDefault:"150"`
	LowWorkers   int           `env:"LOW_PARALLELISM" envDefault:"50"`
	RetryWorkers int           `env:"RETRY_PARALLELISM" envDefault:"50"`
	ProviderURL  string        `env:"PROVIDER_URL" envDefault:"http://localhost:9200"`
	SMTPAddr     string        `env:"SMTP_ADDR" envDefault:"localhost:1025"`
	EmailFrom    string        `env:"EMAIL_FROM" envDefault:"notifications@example.local"`
	ProviderRPS  float64       `env:"PROVIDER_RPS" envDefault:"20000"`
	HighShare    float64       `env:"HIGH_LANE_SHARE" envDefault:"0.7"`
	MaxAttempts  int           `env:"MAX_ATTEMPTS" envDefault:"5"`
	SendTimeout  time.Duration `env:"SEND_TIMEOUT" envDefault:"5s"`
	LimitWait    time.Duration `env:"RATE_LIMIT_WAIT" envDefault:"2s"`
	BatchSize    int           `env:"WORKER_BATCH_SIZE" envDefault:"200"`
}

// main runs a channel worker with priority lanes, retry tiers and a dead-letter topic.
func main() {
	channel := flag.String("channel", "", "channel to serve: push, email, sms, inapp")
	flag.Parse()
	c, err := config.Load[cfg]()
	if err != nil {
		panic(err)
	}
	if *channel != "" {
		c.Channel = *channel
	}
	ch := domain.Channel(c.Channel)
	if !ch.Valid() {
		panic("channel must be push, email, sms or inapp")
	}
	c.ServiceName = "worker-" + c.Channel
	if c.AdminAddr == "" {
		c.AdminAddr = ":9104"
	}

	app.Run(c.Base, nil, func(rt app.Runtime) error {
		awsCfg, err := dynamo.LoadAWS(rt.Ctx, c.Dynamo)
		if err != nil {
			return err
		}
		db := dynamo.NewClient(awsCfg, c.Dynamo)
		tables := dynamo.NewTables(c.TablePrefix)
		rdb := redis.NewClient(&redis.Options{Addr: c.Redis.Addr, PoolSize: 200})
		defer rdb.Close()

		var sender channels.Sender
		switch ch {
		case domain.ChannelPush:
			sender = channels.NewPush(c.ProviderURL)
		case domain.ChannelSMS:
			sender = channels.NewSMS(c.ProviderURL)
		case domain.ChannelEmail:
			sender = &channels.Email{Addr: c.SMTPAddr, From: c.EmailFrom, Timeout: c.SendTimeout}
		case domain.ChannelInApp:
			sender = &channels.InApp{Inbox: dynamo.NewInbox(db, tables), RDB: rdb}
		}

		limiter := func(p domain.Priority, rate float64) worker.Limiter {
			chunk := int(max(1, min(100, rate/10)))
			return ratelimit.New(rdb, fmt.Sprintf("rl:%s:%s", ch, p), rate, int(max(1, rate)), chunk)
		}
		w := &worker.Worker{
			Channel: ch,
			Sender:  sender,
			Dedupe:  worker.RedisDedupe{RDB: rdb},
			Limiters: map[domain.Priority]worker.Limiter{
				domain.PriorityHigh: limiter(domain.PriorityHigh, c.ProviderRPS*c.HighShare),
				domain.PriorityLow:  limiter(domain.PriorityLow, c.ProviderRPS*(1-c.HighShare)),
			},
			Breaker:     worker.NewBreaker("provider-" + c.Channel),
			Devices:     users.NewService(dynamo.NewUsers(db, tables), rdb, 5*time.Minute),
			DeviceID:    dynamo.DeviceID,
			MaxAttempts: c.MaxAttempts,
			SendTimeout: c.SendTimeout,
			LimitWait:   c.LimitWait,
			Now:         time.Now,
		}

		newConsumer := func(group, topic string, parallelism int, retry bool) (*worker.Consumer, error) {
			opts := []kgo.Opt{
				kgo.ConsumerGroup(group),
				kgo.ConsumeTopics(topic),
				kgo.DisableAutoCommit(),
				kgo.FetchMaxWait(250 * time.Millisecond),
			}
			if !retry {
				opts = append(opts, kgo.BlockRebalanceOnPoll())
			}
			kc, err := kafkax.NewClient(c.Brokers, opts...)
			if err != nil {
				return nil, err
			}
			return &worker.Consumer{Worker: w, Client: kc, Log: rt.Log, Parallelism: parallelism, BatchSize: c.BatchSize, Waits: retry}, nil
		}

		var consumers []*worker.Consumer
		defer func() {
			for _, cn := range consumers {
				kafkax.Close(cn.Client, 10*time.Second)
			}
		}()
		add := func(group, topic string, parallelism int, retry bool) error {
			cn, err := newConsumer(group, topic, parallelism, retry)
			if err != nil {
				return err
			}
			consumers = append(consumers, cn)
			return nil
		}
		if err := add("worker-"+c.Channel+"-high", kafkax.DispatchTopic(ch, domain.PriorityHigh), c.HighWorkers, false); err != nil {
			return err
		}
		if err := add("worker-"+c.Channel+"-low", kafkax.DispatchTopic(ch, domain.PriorityLow), c.LowWorkers, false); err != nil {
			return err
		}
		for _, d := range kafkax.RetryTiers {
			if err := add(fmt.Sprintf("worker-%s-retry-%s", c.Channel, d), kafkax.RetryTopic(ch, d), c.RetryWorkers, true); err != nil {
				return err
			}
		}

		g, _ := errgroup.WithContext(rt.Ctx)
		for _, cn := range consumers {
			g.Go(func() error { return cn.Run(rt.Ctx) })
		}
		return g.Wait()
	})
}
