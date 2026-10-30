package app

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"notification-service/internal/config"
	"notification-service/internal/observability"
)

type Runtime struct {
	Ctx context.Context
	Log *slog.Logger
}

// Run wires logging, tracing, the admin server and signal handling, then executes fn.
func Run(base config.Base, ready func(context.Context) error, fn func(rt Runtime) error) {
	log := observability.NewLogger(base.ServiceName, base.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.InitTracing(ctx, base.ServiceName, base.OTLPEndpoint)
	if err != nil {
		log.Error("tracing init failed", "err", err)
		os.Exit(1)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(c)
	}()

	if base.AdminAddr != "" {
		observability.ServeAdmin(ctx, base.AdminAddr, ready, log)
	}

	log.Info("starting")
	if err := fn(Runtime{Ctx: ctx, Log: log}); err != nil {
		log.Error("exited with error", "err", err)
		stop()
		os.Exit(1)
	}
	log.Info("stopped")
}
