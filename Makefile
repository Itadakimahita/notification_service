COMPOSE := docker compose -f deploy/compose/docker-compose.yml

export DYNAMO_ENDPOINT ?= http://localhost:8000
export KAFKA_BROKERS ?= localhost:9092,localhost:9094,localhost:9096
export S3_ENDPOINT ?= http://localhost:9300
export REDIS_ADDR ?= localhost:6380
export OTEL_EXPORTER_OTLP_ENDPOINT ?= localhost:4317

.PHONY: up down logs migrate build test test-unit lint proto tidy

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

migrate:
	go run ./cmd/migrate

build:
	go build -o bin/ ./cmd/...

test:
	go test -race ./...

test-unit:
	go test -race -short ./...

lint:
	go vet ./...
	golangci-lint run ./...

proto:
	buf generate

tidy:
	go mod tidy
