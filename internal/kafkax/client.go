package kafkax

import (
	"time"

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

// Close leaves any group and closes the client, giving up after d so a rebalancing group cannot block shutdown.
func Close(cl *kgo.Client, d time.Duration) {
	done := make(chan struct{})
	go func() {
		cl.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
	}
}
