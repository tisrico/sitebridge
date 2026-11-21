// Package ingest is a minimal stand-in for the cloud telemetry receiver. It
// de-duplicates on (source, stream, seq), records gaps, and can inject
// failures and latency to simulate a poor field network.
package ingest

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/tisrico/sitebridge/internal/telemetry"
)

// Options controls fault injection.
type Options struct {
	FailRate   float64       // probability a request fails with 503
	LoseAcks   float64       // probability the batch is stored but the reply is lost
	Latency    time.Duration // added to every request
	MaxRecords int           // reject larger batches with 413 (0 = unlimited)
}

type streamKey struct{ source, stream string }

type streamState struct {
	lastSeq    int64
	records    int64
	duplicates int64
	gaps       int64
	missing    int64
}

// Server is an http.Handler implementing the ingest API.
type Server struct {
	opts Options
	log  *slog.Logger

	mu      sync.Mutex
	streams map[streamKey]*streamState
	// Received holds every accepted record in arrival order (for tests).
	Received []telemetry.Record
}

// New creates an ingest server.
func New(opts Options, log *slog.Logger) *Server {
	return &Server{opts: opts, log: log, streams: map[streamKey]*streamState{}}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/stats" {
		s.writeStats(w)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.opts.Latency > 0 {
		time.Sleep(s.opts.Latency)
	}
	if s.opts.FailRate > 0 && rand.Float64() < s.opts.FailRate {
		http.Error(w, "injected failure", http.StatusServiceUnavailable)
		return
	}

	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, "bad gzip", http.StatusBadRequest)
			return
		}
		defer zr.Close()
		body = zr
	}
	var b telemetry.Batch
	if err := json.NewDecoder(io.LimitReader(body, 64<<20)).Decode(&b); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if s.opts.MaxRecords > 0 && len(b.Records) > s.opts.MaxRecords {
		http.Error(w, "batch too large", http.StatusRequestEntityTooLarge)
		return
	}

	s.mu.Lock()
	k := streamKey{b.Source, b.Stream}
	st := s.streams[k]
	if st == nil {
		st = &streamState{}
		s.streams[k] = st
	}
	accepted := 0
	for _, rec := range b.Records {
		if rec.Seq <= st.lastSeq {
			st.duplicates++
			continue
		}
		if st.lastSeq != 0 && rec.Seq != st.lastSeq+1 {
			st.gaps++
			st.missing += rec.Seq - st.lastSeq - 1
		}
		st.lastSeq = rec.Seq
		st.records++
		accepted++
		s.Received = append(s.Received, rec)
	}
	ack := st.lastSeq
	s.mu.Unlock()

	if accepted > 0 {
		s.log.Info("batch stored", "source", b.Source, "stream", b.Stream, "records", accepted, "ack", ack)
	}
	if s.opts.LoseAcks > 0 && rand.Float64() < s.opts.LoseAcks {
		// Stored, but the client never hears back: it must resend, and we
		// must de-duplicate.
		panic(http.ErrAbortHandler)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(telemetry.AckResponse{Ack: ack})
}

// Stats is a summary per stream.
type Stats struct {
	Source     string `json:"source"`
	Stream     string `json:"stream"`
	LastSeq    int64  `json:"last_seq"`
	Records    int64  `json:"records"`
	Duplicates int64  `json:"duplicates"`
	Gaps       int64  `json:"gaps"`
	Missing    int64  `json:"missing"`
}

// Stats returns per-stream counters.
func (s *Server) Stats() []Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Stats
	for k, st := range s.streams {
		out = append(out, Stats{k.source, k.stream, st.lastSeq, st.records, st.duplicates, st.gaps, st.missing})
	}
	return out
}

func (s *Server) writeStats(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(s.Stats())
}
