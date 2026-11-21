// Package backoff implements capped exponential backoff with jitter.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Backoff doubles the delay after each failure up to Max. Each delay is
// jittered to between 50% and 100% of the nominal value, so a fleet of
// devices that lost the same network does not reconnect in lockstep.
type Backoff struct {
	Min, Max time.Duration
	cur      time.Duration
	// Jitter returns a factor in [0.5, 1). Replaceable in tests.
	Jitter func() float64
}

// New returns a backoff between min and max.
func New(min, max time.Duration) *Backoff {
	return &Backoff{Min: min, Max: max, Jitter: func() float64 { return 0.5 + rand.Float64()/2 }}
}

// Next returns the next delay.
func (b *Backoff) Next() time.Duration {
	if b.cur == 0 {
		b.cur = b.Min
	} else {
		b.cur *= 2
		if b.cur > b.Max {
			b.cur = b.Max
		}
	}
	return time.Duration(float64(b.cur) * b.Jitter())
}

// Reset returns to the minimum delay after a success.
func (b *Backoff) Reset() { b.cur = 0 }
