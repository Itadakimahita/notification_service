package kafkax

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// NewClient creates a franz-go client tuned for durable, idempotent production.
func NewClient(brokers []string, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression()),
	}
	return kgo.NewClient(append(base, opts...)...)
}
