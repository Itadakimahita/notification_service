package config

import "github.com/caarlos0/env/v11"

type Base struct {
	ServiceName  string `env:"SERVICE_NAME"`
	LogLevel     string `env:"LOG_LEVEL" envDefault:"info"`
	AdminAddr    string `env:"ADMIN_ADDR"`
	OTLPEndpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
}

type Kafka struct {
	Brokers []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"localhost:9092,localhost:9094,localhost:9096"`
}

type Dynamo struct {
	Endpoint    string `env:"DYNAMO_ENDPOINT"`
	Region      string `env:"AWS_REGION" envDefault:"us-east-1"`
	TablePrefix string `env:"DYNAMO_TABLE_PREFIX" envDefault:"notif_"`
}

type Redis struct {
	Addr string `env:"REDIS_ADDR" envDefault:"localhost:6380"`
}

// Load parses environment variables into a new T.
func Load[T any]() (T, error) {
	var c T
	err := env.Parse(&c)
	return c, err
}
