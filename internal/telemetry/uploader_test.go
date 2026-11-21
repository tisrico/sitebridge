package telemetry_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tisrico/sitebridge/internal/ingest"
	"github.com/tisrico/sitebridge/internal/telemetry"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func setup(t *testing.T, n int) *telemetry.Outbox {
	t.Helper()
	ob, err := telemetry.OpenOutbox(filepath.Join(t.TempDir(), "o.db"), "NORMAL", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ob.Close() })
	for i := range n {
		if _, err := ob.Append(context.Background(), time.Now(), []byte(fmt.Sprintf(`{"i":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	return ob
}

func run(t *testing.T, ob *telemetry.Outbox, url string, batch int) (*telemetry.Uploader, context.CancelFunc) {
	t.Helper()
	u := telemetry.NewUploader(telemetry.UploaderConfig{
		URL: url, Source: "test", BatchMax: batch,
		FlushInterval: 5 * time.Millisecond, RequestTimeout: time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 10 * time.Millisecond,
	}, ob, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { u.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return u, cancel
}

func waitDrained(t *testing.T, ob *telemetry.Outbox) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for ob.Len() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("outbox not drained: %d left", ob.Len())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func checkExactlyOnceInOrder(t *testing.T, srv *ingest.Server, n int) {
	t.Helper()
	if len(srv.Received) != n {
		t.Fatalf("receiver stored %d records, want %d", len(srv.Received), n)
	}
	for i, r := range srv.Received {
		if r.Seq != int64(i+1) {
			t.Fatalf("record %d has seq %d: out of order or duplicated", i, r.Seq)
		}
	}
}

// Flaky network: half of requests fail and a third of successful replies are
// lost. Every record must still arrive exactly once (after de-duplication) and
// in order.
func TestUploadOverFlakyNetwork(t *testing.T) {
	ob := setup(t, 300)
	srv := ingest.New(ingest.Options{FailRate: 0.5, LoseAcks: 0.3}, quiet())
	hs := httptest.NewServer(srv)
	defer hs.Close()

	run(t, ob, hs.URL, 50)
	waitDrained(t, ob)
	checkExactlyOnceInOrder(t, srv, 300)
	st := srv.Stats()[0]
	t.Logf("duplicates de-duplicated by receiver: %d", st.Duplicates)
	if st.Gaps != 0 {
		t.Fatalf("receiver saw %d gaps", st.Gaps)
	}
}

// Cloud unreachable for a while, then back: nothing is lost or acked early.
func TestUploadOutageThenRecovery(t *testing.T) {
	ob := setup(t, 120)
	srv := ingest.New(ingest.Options{}, quiet())
	var up atomic.Bool
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		srv.ServeHTTP(w, r)
	}))
	defer hs.Close()

	u, _ := run(t, ob, hs.URL, 25)
	time.Sleep(100 * time.Millisecond)
	if ob.Len() != 120 {
		t.Fatalf("records removed during outage: %d left", ob.Len())
	}
	if u.Stats().Failures == 0 {
		t.Fatal("expected upload failures during outage")
	}

	// New data keeps arriving during the outage.
	for range 30 {
		ob.Append(context.Background(), time.Now(), []byte(`{}`))
	}
	up.Store(true)
	waitDrained(t, ob)
	checkExactlyOnceInOrder(t, srv, 150)
}

// Receiver rejects big batches with 413: the uploader shrinks and continues.
func TestUploadShrinksOnTooLarge(t *testing.T) {
	ob := setup(t, 200)
	srv := ingest.New(ingest.Options{MaxRecords: 30}, quiet())
	hs := httptest.NewServer(srv)
	defer hs.Close()

	run(t, ob, hs.URL, 100)
	waitDrained(t, ob)
	checkExactlyOnceInOrder(t, srv, 200)
}
