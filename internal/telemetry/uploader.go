package telemetry

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/tisrico/sitebridge/internal/backoff"
)

// Batch is the wire format posted to the ingest endpoint.
type Batch struct {
	Source  string   `json:"source"`
	Stream  string   `json:"stream"`
	Records []Record `json:"records"`
}

// AckResponse is the ingest endpoint's reply. Ack is the highest sequence
// number the receiver has durably stored; it may be lower than the last
// record sent (partial acknowledgement).
type AckResponse struct {
	Ack int64 `json:"ack"`
}

// UploaderConfig configures an Uploader.
type UploaderConfig struct {
	URL            string
	Source         string
	BatchMax       int
	FlushInterval  time.Duration
	RequestTimeout time.Duration
	BackoffMin     time.Duration
	BackoffMax     time.Duration
	Client         *http.Client
}

// Uploader drains the outbox to the cloud in order.
type Uploader struct {
	cfg   UploaderConfig
	ob    *Outbox
	log   *slog.Logger
	nudge chan struct{}

	mu       sync.Mutex
	batchMax int // current limit; shrinks on 413 and recovers on success
	stats    UploaderStats
}

// UploaderStats is reported on the status endpoint.
type UploaderStats struct {
	Batches     int64     `json:"batches"`
	Records     int64     `json:"records"`
	Failures    int64     `json:"failures"`
	LastSuccess time.Time `json:"last_success"`
	LastError   string    `json:"last_error,omitempty"`
	BatchMax    int       `json:"batch_max"`
}

// NewUploader creates an uploader.
func NewUploader(cfg UploaderConfig, ob *Outbox, log *slog.Logger) *Uploader {
	if cfg.Client == nil {
		cfg.Client = &http.Client{}
	}
	return &Uploader{cfg: cfg, ob: ob, log: log, nudge: make(chan struct{}, 1), batchMax: cfg.BatchMax}
}

// Nudge asks the uploader to flush now instead of waiting for the interval.
func (u *Uploader) Nudge() {
	select {
	case u.nudge <- struct{}{}:
	default:
	}
}

// Stats returns a copy of the counters.
func (u *Uploader) Stats() UploaderStats {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := u.stats
	s.BatchMax = u.batchMax
	return s
}

// errTooLarge means the receiver rejected the batch size; retry smaller.
var errTooLarge = errors.New("batch too large")

// Run uploads until ctx is cancelled. Records leave the outbox only when
// acknowledged, strictly in sequence order. While the backlog is larger than
// one batch, batches are sent back to back so a long outage drains quickly.
func (u *Uploader) Run(ctx context.Context) {
	bo := backoff.New(u.cfg.BackoffMin, u.cfg.BackoffMax)
	wait := func(d time.Duration) bool {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		case <-u.nudge:
		}
		return true
	}

	for {
		u.mu.Lock()
		limit := u.batchMax
		u.mu.Unlock()

		recs, err := u.ob.Peek(ctx, limit)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			u.log.Error("outbox read failed", "err", err)
			if !wait(bo.Next()) {
				return
			}
			continue
		}
		if len(recs) == 0 {
			if !wait(u.cfg.FlushInterval) {
				return
			}
			continue
		}

		acked, err := u.send(ctx, recs)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			u.recordFailure(err, len(recs))
			if errors.Is(err, errTooLarge) {
				continue // retry immediately with the smaller batch
			}
			d := bo.Next()
			u.log.Warn("upload failed; backing off", "err", err, "retry_in", d.Round(time.Millisecond), "backlog", u.ob.Len())
			if !wait(d) {
				return
			}
			continue
		}

		if err := u.ob.Ack(ctx, acked); err != nil {
			u.log.Error("outbox ack failed", "err", err)
		}
		bo.Reset()
		u.recordSuccess(recs, acked)

		if len(recs) == limit && acked == recs[len(recs)-1].Seq {
			continue // more backlog: keep draining
		}
		if !wait(u.cfg.FlushInterval) {
			return
		}
	}
}

func (u *Uploader) recordFailure(err error, n int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.stats.Failures++
	u.stats.LastError = err.Error()
	if errors.Is(err, errTooLarge) && u.batchMax > 1 {
		u.batchMax = max(1, min(u.batchMax, n)/2)
		u.log.Warn("receiver rejected batch size; shrinking", "batch_max", u.batchMax)
	}
}

func (u *Uploader) recordSuccess(recs []Record, acked int64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	var n int64
	for _, r := range recs {
		if r.Seq <= acked {
			n++
		}
	}
	u.stats.Batches++
	u.stats.Records += n
	u.stats.LastSuccess = time.Now()
	u.stats.LastError = ""
	// Grow back towards the configured size after a shrink.
	if u.batchMax < u.cfg.BatchMax {
		u.batchMax = min(u.cfg.BatchMax, u.batchMax*2)
	}
}

func (u *Uploader) send(ctx context.Context, recs []Record) (int64, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if err := json.NewEncoder(zw).Encode(Batch{Source: u.cfg.Source, Stream: u.ob.StreamID(), Records: recs}); err != nil {
		return 0, err
	}
	if err := zw.Close(); err != nil {
		return 0, err
	}

	rctx, cancel := context.WithTimeout(ctx, u.cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, u.cfg.URL, &buf)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := u.cfg.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))

	switch {
	case resp.StatusCode == http.StatusRequestEntityTooLarge:
		return 0, errTooLarge
	case resp.StatusCode != http.StatusOK:
		return 0, fmt.Errorf("ingest returned %s", resp.Status)
	}
	var ack AckResponse
	if err := json.Unmarshal(body, &ack); err != nil {
		return 0, fmt.Errorf("bad ack: %w", err)
	}
	first, last := recs[0].Seq, recs[len(recs)-1].Seq
	if ack.Ack > last {
		// The receiver already holds more than this batch, e.g. a resend
		// after a lost ack with a smaller batch size. Everything sent is safe.
		ack.Ack = last
	}
	if ack.Ack < first-1 {
		return 0, fmt.Errorf("ack %d before batch start %d", ack.Ack, first)
	}
	return ack.Ack, nil
}
