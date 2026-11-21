package telemetry

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func openTemp(t *testing.T, max int) (*Outbox, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "outbox.db")
	ob, err := OpenOutbox(path, "FULL", max)
	if err != nil {
		t.Fatal(err)
	}
	return ob, path
}

func TestOutboxFIFOAndAck(t *testing.T) {
	ctx := context.Background()
	ob, _ := openTemp(t, 0)
	defer ob.Close()
	for i := range 10 {
		if _, err := ob.Append(ctx, time.Now(), []byte(fmt.Sprintf(`{"i":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	recs, err := ob.Peek(ctx, 4)
	if err != nil || len(recs) != 4 || recs[0].Seq != 1 || recs[3].Seq != 4 {
		t.Fatalf("peek: %v %+v", err, recs)
	}
	if err := ob.Ack(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if ob.Len() != 6 {
		t.Fatalf("len after ack = %d", ob.Len())
	}
	recs, _ = ob.Peek(ctx, 100)
	if recs[0].Seq != 5 || string(recs[0].Data) != `{"i":4}` {
		t.Fatalf("after ack first = %+v", recs[0])
	}
}

func TestOutboxSurvivesReopenAndNeverReusesSeq(t *testing.T) {
	ctx := context.Background()
	ob, path := openTemp(t, 0)
	for range 3 {
		ob.Append(ctx, time.Now(), []byte(`{}`))
	}
	stream := ob.StreamID()
	ob.Ack(ctx, 3) // fully drained
	ob.Close()

	ob, err := OpenOutbox(path, "FULL", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ob.Close()
	if ob.StreamID() != stream {
		t.Fatalf("stream id changed across reopen")
	}
	seq, _ := ob.Append(ctx, time.Now(), []byte(`{}`))
	if seq != 4 {
		t.Fatalf("seq after drain and reopen = %d, want 4 (never reused)", seq)
	}
}

func TestOutboxBacklogCapDropsOldest(t *testing.T) {
	ctx := context.Background()
	ob, _ := openTemp(t, 5)
	defer ob.Close()
	for range 8 {
		ob.Append(ctx, time.Now(), []byte(`{}`))
	}
	if ob.Len() != 5 || ob.Dropped() != 3 {
		t.Fatalf("len=%d dropped=%d, want 5 and 3", ob.Len(), ob.Dropped())
	}
	recs, _ := ob.Peek(ctx, 10)
	if recs[0].Seq != 4 || recs[4].Seq != 8 {
		t.Fatalf("kept %d..%d, want 4..8", recs[0].Seq, recs[4].Seq)
	}
}

// TestOutboxSurvivesKill9 repeatedly SIGKILLs a child process in the middle
// of a write loop, then checks the database opens, passes SQLite's integrity
// check, and holds a gap-free sequence.
func TestOutboxSurvivesKill9(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	path := filepath.Join(t.TempDir(), "crash.db")
	for round := range 5 {
		cmd := exec.Command(os.Args[0], "-test.run=TestCrashHelper")
		cmd.Env = append(os.Environ(), "SITEBRIDGE_CRASH_DB="+path)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(150+round*50) * time.Millisecond)
		cmd.Process.Kill()
		cmd.Wait()

		ob, err := OpenOutbox(path, "FULL", 0)
		if err != nil {
			t.Fatalf("round %d: reopen: %v", round, err)
		}
		if err := ob.IntegrityCheck(); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		recs, err := ob.Peek(context.Background(), 1<<30)
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < len(recs); i++ {
			if recs[i].Seq != recs[i-1].Seq+1 {
				t.Fatalf("round %d: gap between %d and %d", round, recs[i-1].Seq, recs[i].Seq)
			}
		}
		if len(recs) != ob.Len() {
			t.Fatalf("round %d: count mismatch", round)
		}
		t.Logf("round %d: %d records intact", round, len(recs))
		ob.Close()
	}
}

// TestCrashHelper is the child process for TestOutboxSurvivesKill9.
func TestCrashHelper(t *testing.T) {
	path := os.Getenv("SITEBRIDGE_CRASH_DB")
	if path == "" {
		t.Skip("helper process only")
	}
	ob, err := OpenOutbox(path, "FULL", 0)
	if err != nil {
		os.Exit(2)
	}
	payload := []byte(`{"bess1.soc":{"v":80.1,"q":"good","ts":0}}`)
	for {
		if _, err := ob.Append(context.Background(), time.Now(), payload); err != nil {
			os.Exit(3)
		}
	}
}
