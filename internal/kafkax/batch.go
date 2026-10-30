package kafkax

import (
	"context"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Handler func(ctx context.Context, rec *kgo.Record) []*kgo.Record

type BatchFunc func(ctx context.Context, recs []*kgo.Record) ([]*kgo.Record, error)

// RunBatches polls batches, lets fn process each one, produces its outputs, then commits.
func RunBatches(ctx context.Context, cl *kgo.Client, batchSize int, fn BatchFunc, log *slog.Logger) error {
	defer cl.AllowRebalance()
	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, batchSize)
		if ctx.Err() != nil {
			break
		}
		fetches.EachError(func(t string, p int32, err error) { log.Error("fetch error", "topic", t, "partition", p, "err", err) })

		recs := fetches.Records()
		if len(recs) == 0 {
			cl.AllowRebalance()
			continue
		}

		outputs, err := fn(ctx, recs)
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			break
		}
		if len(outputs) > 0 {
			if err := cl.ProduceSync(ctx, outputs...).FirstErr(); err != nil {
				return err
			}
		}
		if err := cl.CommitRecords(ctx, recs...); err != nil && ctx.Err() == nil {
			log.Error("commit failed", "err", err)
		}
		cl.AllowRebalance()
	}
	return nil
}

// ProcessBatches handles every record of a batch in parallel and collects their outputs.
func ProcessBatches(ctx context.Context, cl *kgo.Client, batchSize, parallelism int, handle Handler, log *slog.Logger) error {
	return RunBatches(ctx, cl, batchSize, func(ctx context.Context, recs []*kgo.Record) ([]*kgo.Record, error) {
		var (
			mu      sync.Mutex
			outputs []*kgo.Record
			wg      sync.WaitGroup
			sem     = make(chan struct{}, parallelism)
		)
		for _, rec := range recs {
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				out := handle(ctx, rec)
				mu.Lock()
				outputs = append(outputs, out...)
				mu.Unlock()
			}()
		}
		wg.Wait()
		return outputs, nil
	}, log)
}
