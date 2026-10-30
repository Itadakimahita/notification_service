package campaign

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"

	"notification-service/internal/blob"
	"notification-service/internal/domain"
	"notification-service/internal/store/dynamo"
)

const maxChunk = 500

var emittedTotal = promauto.NewCounter(prometheus.CounterOpts{Name: "campaign_emitted_total", Help: "Campaign recipients accepted."})

type Source interface {
	Get(ctx context.Context, key string, offset int64) (io.ReadCloser, error)
}

type Emitter interface {
	Create(ctx context.Context, n *domain.Notification, idemKey string) (string, bool, error)
}

type Store interface {
	Get(ctx context.Context, id string) (*dynamo.Campaign, error)
	SetStatus(ctx context.Context, id string, from []dynamo.CampaignStatus, to dynamo.CampaignStatus, errMsg string) error
	Progress(ctx context.Context, id string, offset, emitted, skipped int64) error
	ListRunning(ctx context.Context) ([]dynamo.Campaign, error)
}

type Leases interface {
	Acquire(ctx context.Context, name, owner string) (dynamo.Lease, bool, error)
	Checkpoint(ctx context.Context, name, owner, checkpoint string, done bool) (bool, error)
}

type Runner struct {
	Store   Store
	Source  Source
	Emitter Emitter
	Leases  Leases
	Owner   string
	Log     *slog.Logger
	Poll    time.Duration
	Workers int
	Now     func() time.Time

	mu      sync.Mutex
	running map[string]bool
}

// Run picks up RUNNING campaigns and streams each one until completion, pause or cancel.
func (r *Runner) Run(ctx context.Context) error {
	r.running = map[string]bool{}
	var wg sync.WaitGroup
	defer wg.Wait()
	tick := time.NewTicker(r.Poll)
	defer tick.Stop()
	for {
		camps, err := r.Store.ListRunning(ctx)
		if err != nil && ctx.Err() == nil {
			r.Log.Error("list campaigns failed", "err", err)
		}
		for _, c := range camps {
			if !r.claim(c.ID) {
				continue
			}
			if _, ok, err := r.Leases.Acquire(ctx, "campaign#"+c.ID, r.Owner); err != nil || !ok {
				r.release(c.ID)
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer r.release(c.ID)
				if err := r.RunCampaign(ctx, c.ID); err != nil && ctx.Err() == nil {
					r.Log.Error("campaign stopped", "campaign_id", c.ID, "err", err)
				}
			}()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func (r *Runner) claim(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running[id] {
		return false
	}
	r.running[id] = true
	return true
}

func (r *Runner) release(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.running, id)
}

type row struct {
	userID string
	data   map[string]string
}

// RunCampaign streams the audience from its saved offset in rate-limited chunks, checkpointing after each.
func (r *Runner) RunCampaign(ctx context.Context, id string) error {
	c, err := r.Store.Get(ctx, id)
	if err != nil || c.Status != dynamo.CampaignRunning {
		return err
	}
	columns, err := r.header(ctx, c)
	if err != nil {
		return r.fail(ctx, c, err)
	}
	body, err := r.Source.Get(ctx, c.AudienceKey, c.Offset)
	if err != nil {
		return r.fail(ctx, c, err)
	}
	defer body.Close()
	rd := bufio.NewReaderSize(body, 1<<20)
	if c.Offset == 0 {
		line, _, err := readLine(rd)
		if err != nil && line == "" {
			return r.fail(ctx, c, errors.New("audience file is empty"))
		}
		if !isHeader(line) {
			rd = bufio.NewReaderSize(io.MultiReader(strings.NewReader(line+"\n"), rd), 1<<20)
		} else {
			c.Offset += int64(len(line)) + 1
		}
	}

	chunk := max(1, min(maxChunk, c.RatePerSec/2))
	limiter := rate.NewLimiter(rate.Limit(c.RatePerSec), chunk)
	lease := "campaign#" + id
	offset, emitted, skipped := c.Offset, c.Emitted, c.Skipped

	for ctx.Err() == nil {
		rows, consumed, skippedNow, eof := readChunk(rd, columns, chunk)
		n, err := r.emit(ctx, c, rows, limiter)
		if err != nil {
			return err
		}
		offset += consumed
		emitted += n
		skipped += skippedNow
		emittedTotal.Add(float64(n))

		if eof {
			if err := r.Store.Progress(ctx, id, offset, emitted, skipped); err != nil {
				return err
			}
			return r.Store.SetStatus(ctx, id, []dynamo.CampaignStatus{dynamo.CampaignRunning}, dynamo.CampaignCompleted, "")
		}
		if err := r.Store.Progress(ctx, id, offset, emitted, skipped); err != nil {
			return err
		}
		if held, err := r.Leases.Checkpoint(ctx, lease, r.Owner, "", false); err != nil || !held {
			return err
		}
		cur, err := r.Store.Get(ctx, id)
		if err != nil || cur.Status != dynamo.CampaignRunning {
			return err
		}
	}
	return nil
}

func (r *Runner) fail(ctx context.Context, c *dynamo.Campaign, cause error) error {
	msg := cause.Error()
	if errors.Is(cause, blob.ErrNotFound) {
		msg = "audience file not found"
	}
	if err := r.Store.SetStatus(ctx, c.ID, []dynamo.CampaignStatus{dynamo.CampaignRunning}, dynamo.CampaignFailed, msg); err != nil {
		return err
	}
	return cause
}

// header reads the column names so a resumed run can parse rows after the first line.
func (r *Runner) header(ctx context.Context, c *dynamo.Campaign) ([]string, error) {
	body, err := r.Source.Get(ctx, c.AudienceKey, 0)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	line, _, _ := readLine(bufio.NewReader(body))
	if !isHeader(line) {
		return []string{"user_id"}, nil
	}
	cols, err := csv.NewReader(strings.NewReader(line)).Read()
	if err != nil {
		return nil, err
	}
	for i := range cols {
		cols[i] = strings.TrimSpace(cols[i])
	}
	return cols, nil
}

func isHeader(line string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(line)), "user_id")
}

func readLine(rd *bufio.Reader) (string, int64, error) {
	line, err := rd.ReadString('\n')
	n := int64(len(line))
	return strings.TrimRight(line, "\r\n"), n, err
}

// readChunk reads up to n lines and reports bytes consumed, malformed lines skipped, and end of file.
func readChunk(rd *bufio.Reader, columns []string, n int) ([]row, int64, int64, bool) {
	var rows []row
	var consumed, skipped int64
	for len(rows) < n {
		line, size, err := readLine(rd)
		consumed += size
		if strings.TrimSpace(line) != "" {
			fields, perr := csv.NewReader(strings.NewReader(line)).Read()
			if perr != nil || len(fields) == 0 || strings.TrimSpace(fields[0]) == "" {
				skipped++
			} else {
				rw := row{userID: strings.TrimSpace(fields[0]), data: map[string]string{}}
				for i := 1; i < len(fields) && i < len(columns); i++ {
					rw.data[columns[i]] = fields[i]
				}
				rows = append(rows, rw)
			}
		}
		if err != nil {
			return rows, consumed, skipped, true
		}
	}
	return rows, consumed, skipped, false
}

// emit creates one notification per row through the normal accept path, respecting the rate limit.
func (r *Runner) emit(ctx context.Context, c *dynamo.Campaign, rows []row, limiter *rate.Limiter) (int64, error) {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		count   int64
		firstEr error
		sem     = make(chan struct{}, r.Workers)
	)
	producer := "campaign:" + c.ID
	channels := make([]domain.Channel, 0, len(c.Channels))
	for _, ch := range c.Channels {
		channels = append(channels, domain.Channel(ch))
	}
	for _, rw := range rows {
		if ctx.Err() != nil {
			break
		}
		if err := limiter.Wait(ctx); err != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			data := make(map[string]string, len(c.Data)+len(rw.data))
			for k, v := range c.Data {
				data[k] = v
			}
			for k, v := range rw.data {
				data[k] = v
			}
			n := domain.NewNotification(producer, rw.userID, c.TemplateID, domain.Category(c.Category), domain.PriorityLow, channels, data, r.Now())
			if _, _, err := r.Emitter.Create(ctx, n, rw.userID); err != nil {
				mu.Lock()
				if firstEr == nil {
					firstEr = err
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			count++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if firstEr != nil {
		return count, firstEr
	}
	return count, ctx.Err()
}
