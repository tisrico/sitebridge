package backoff

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	b := New(time.Second, 8*time.Second)
	b.Jitter = func() float64 { return 1 }
	want := []time.Duration{1, 2, 4, 8, 8}
	for i, w := range want {
		if got := b.Next(); got != w*time.Second {
			t.Errorf("step %d: got %v want %v", i, got, w*time.Second)
		}
	}
	b.Reset()
	if got := b.Next(); got != time.Second {
		t.Errorf("after reset: got %v", got)
	}
}

func TestJitterBounds(t *testing.T) {
	b := New(time.Second, time.Second)
	for range 1000 {
		d := b.Next()
		if d < 500*time.Millisecond || d > time.Second {
			t.Fatalf("jittered delay %v out of bounds", d)
		}
	}
}
