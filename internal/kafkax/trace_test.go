package kafkax

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceSurvivesKafkaHop(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.TraceContext{})

	ctx, parent := otel.Tracer("a").Start(context.Background(), "producer")
	msg := &kgo.Record{Headers: []kgo.RecordHeader{{Key: "notification_id", Value: []byte("n1")}}}
	InjectTrace(ctx, msg)
	parent.End()
	require.Len(t, msg.Headers, 2)

	childCtx, child := otel.Tracer("b").Start(ExtractTrace(context.Background(), msg), "consumer")
	child.End()

	require.Equal(t, parent.SpanContext().TraceID(), trace.SpanContextFromContext(childCtx).TraceID())
	spans := rec.Ended()
	require.Len(t, spans, 2)
	require.Equal(t, parent.SpanContext().SpanID(), spans[1].Parent().SpanID())

	InjectTrace(ctx, msg)
	require.Len(t, msg.Headers, 2, "injecting again must replace, not duplicate, the header")
}
