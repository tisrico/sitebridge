// Command mockcloud is a stand-in telemetry ingest service with fault
// injection, for exercising the bridge's store-and-forward path.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/tisrico/sitebridge/internal/ingest"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "listen address")
	failRate := flag.Float64("fail-rate", 0, "fraction of requests to fail with 503")
	loseAcks := flag.Float64("lose-acks", 0, "fraction of stored batches whose reply is dropped")
	latency := flag.Duration("latency", 0, "latency added to each request")
	maxRecords := flag.Int("max-records", 0, "reject batches larger than this with 413 (0 = no limit)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	srv := ingest.New(ingest.Options{FailRate: *failRate, LoseAcks: *loseAcks, Latency: *latency, MaxRecords: *maxRecords}, log)
	log.Info("mock cloud listening", "addr", *addr, "ingest", "POST /ingest", "stats", "GET /stats")
	hs := &http.Server{Addr: *addr, Handler: srv, ReadHeaderTimeout: 5 * time.Second}
	if err := hs.ListenAndServe(); err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
}
