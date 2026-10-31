package kafkax

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
)

type headerCarrier struct{ rec *kgo.Record }

func (c headerCarrier) Get(key string) string {
	for _, h := range c.rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range c.rec.Headers {
		if h.Key == key {
			c.rec.Headers[i].Value = []byte(value)
			return
		}
	}
	c.rec.Headers = append(c.rec.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(c.rec.Headers))
	for _, h := range c.rec.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// InjectTrace writes the trace context of ctx into the record headers.
func InjectTrace(ctx context.Context, rec *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier{rec})
}

// ExtractTrace returns ctx carrying the trace context found in the record headers.
func ExtractTrace(ctx context.Context, rec *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{rec})
}
