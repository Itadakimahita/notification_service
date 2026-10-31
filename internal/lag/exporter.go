package lag

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
)

var (
	consumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_consumer_lag", Help: "Messages behind the log end per consumer group and topic."}, []string{"group", "topic"})
	dlqDepth    = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "notification_dlq_depth", Help: "Messages retained in each dead-letter topic (replay does not delete); alert on growth."}, []string{"topic"})
)

var groupPrefixes = []string{"router", "status", "worker-"}

type Exporter struct {
	Adm   *kadm.Client
	Log   *slog.Logger
	Every time.Duration
}

// Run refreshes lag and DLQ gauges on an interval until ctx ends.
func (e *Exporter) Run(ctx context.Context) error {
	tick := time.NewTicker(e.Every)
	defer tick.Stop()
	for {
		if err := e.collect(ctx); err != nil && ctx.Err() == nil {
			e.Log.Error("lag collection failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func (e *Exporter) collect(ctx context.Context) error {
	listed, err := e.Adm.ListGroups(ctx)
	if err != nil {
		return err
	}
	var groups []string
	for _, g := range listed.Groups() {
		for _, p := range groupPrefixes {
			if strings.HasPrefix(g, p) {
				groups = append(groups, g)
				break
			}
		}
	}
	sort.Strings(groups)

	lags, err := e.Adm.Lag(ctx, groups...)
	if err != nil {
		return err
	}
	consumerLag.Reset()
	for group, l := range lags {
		if l.Error() != nil {
			continue
		}
		for topic, tl := range l.Lag.TotalByTopic() {
			consumerLag.WithLabelValues(group, topic).Set(float64(tl.Lag))
		}
	}

	topics, err := e.Adm.ListTopics(ctx)
	if err != nil {
		return err
	}
	var dlqs []string
	for _, t := range topics.Names() {
		if strings.HasPrefix(t, "notifications.dlq.") {
			dlqs = append(dlqs, t)
		}
	}
	starts, err := e.Adm.ListStartOffsets(ctx, dlqs...)
	if err != nil {
		return err
	}
	ends, err := e.Adm.ListEndOffsets(ctx, dlqs...)
	if err != nil {
		return err
	}
	dlqDepth.Reset()
	depth := map[string]int64{}
	ends.Each(func(o kadm.ListedOffset) { depth[o.Topic] += o.Offset })
	starts.Each(func(o kadm.ListedOffset) { depth[o.Topic] -= o.Offset })
	for _, t := range dlqs {
		dlqDepth.WithLabelValues(t).Set(float64(depth[t]))
	}
	return nil
}
