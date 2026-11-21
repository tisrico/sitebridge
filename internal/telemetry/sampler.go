package telemetry

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/tisrico/sitebridge/internal/tagstore"
)

// Sample is one tag in a telemetry snapshot.
type Sample struct {
	V  float64 `json:"v"`
	Q  string  `json:"q"`
	TS int64   `json:"ts"` // unix ms of the reading
}

// RunSampler writes a snapshot of every tag to the outbox at a fixed
// interval. Sampling is decoupled from device polling, so the upload volume is
// set by telemetry policy rather than by how fast a device is polled.
func RunSampler(ctx context.Context, interval time.Duration, store *tagstore.Store, ob *Outbox, onAppend func(), log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			snap := store.Snapshot()
			out := make(map[string]Sample, len(snap))
			for k, v := range snap {
				out[k] = Sample{V: v.V, Q: v.Q.String(), TS: v.TS.UnixMilli()}
			}
			b, err := json.Marshal(out)
			if err != nil {
				log.Error("encode sample", "err", err)
				continue
			}
			if _, err := ob.Append(ctx, now, b); err != nil {
				if ctx.Err() == nil {
					log.Error("outbox append failed", "err", err)
				}
				continue
			}
			if onAppend != nil {
				onAppend()
			}
		}
	}
}
