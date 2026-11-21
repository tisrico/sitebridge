package modbusdev

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/simonvetter/modbus"

	"github.com/tisrico/sitebridge/internal/backoff"
	"github.com/tisrico/sitebridge/internal/config"
	"github.com/tisrico/sitebridge/internal/tagstore"
)

// Client is the subset of a Modbus client the poller needs.
type Client interface {
	Open() error
	Close() error
	SetUnitId(id uint8) error
	ReadRegisters(addr, quantity uint16, regType modbus.RegType) ([]uint16, error)
	WriteRegisters(addr uint16, values []uint16) error
}

// Dialer creates a client for a device. Replaceable in tests.
type Dialer func(d config.Device) (Client, error)

// TCPDialer is the production dialer.
func TCPDialer(d config.Device) (Client, error) {
	return modbus.NewClient(&modbus.ClientConfiguration{
		URL: d.Address, Timeout: d.Timeout,
		Logger: log.New(io.Discard, "", 0), // the poller logs errors itself, once
	})
}

// ErrNotConnected is returned for writes while the device link is down.
var ErrNotConnected = errors.New("device not connected")

type writeReq struct {
	tag   int
	value float64
	res   chan error
}

// Poller owns the single connection to one device. All reads and writes go
// through its goroutine, so there is exactly one Modbus master per device per
// node and no request interleaving on the wire.
type Poller struct {
	dev    config.Device
	blocks []Block
	byName map[string]int
	store  *tagstore.Store
	dial   Dialer
	log    *slog.Logger
	writes chan writeReq

	client Client

	polls, pollErrors, reconnects, writesOK, writeErrors atomic.Int64
	connected                                            atomic.Bool
	mu                                                   sync.Mutex
	lastErr                                              string
	lastPoll                                             time.Time
	lastPollDuration                                     time.Duration
}

// NewPoller creates a poller for one device.
func NewPoller(dev config.Device, store *tagstore.Store, dial Dialer, log *slog.Logger) *Poller {
	byName := make(map[string]int, len(dev.Tags))
	for i, t := range dev.Tags {
		byName[t.Name] = i
	}
	return &Poller{
		dev:    dev,
		blocks: PlanReads(dev.Tags, dev.MaxGap),
		byName: byName,
		store:  store,
		dial:   dial,
		log:    log.With("device", dev.Name),
		writes: make(chan writeReq),
	}
}

// Name returns the device name.
func (p *Poller) Name() string { return p.dev.Name }

// Run polls until ctx is cancelled. On exit the device's tags are marked
// out of service and the connection is closed.
func (p *Poller) Run(ctx context.Context) {
	p.log.Info("poller started", "address", p.dev.Address, "blocks", len(p.blocks))
	defer func() {
		p.disconnect()
		p.store.SetQuality(p.dev.Name+".", tagstore.OutOfService)
		p.log.Info("poller stopped")
	}()

	bo := backoff.New(500*time.Millisecond, 30*time.Second)
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case w := <-p.writes:
			w.res <- p.doWrite(w)
		case <-timer.C:
			if p.client == nil {
				if err := p.connect(); err != nil {
					p.recordErr(err)
					p.store.SetQuality(p.dev.Name+".", tagstore.CommFailure)
					timer.Reset(bo.Next())
					continue
				}
			}
			if err := p.pollOnce(); err != nil {
				timer.Reset(bo.Next())
				continue
			}
			bo.Reset()
			timer.Reset(p.dev.PollInterval)
		}
	}
}

func (p *Poller) connect() error {
	c, err := p.dial(p.dev)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	if err := c.Open(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := c.SetUnitId(p.dev.UnitID); err != nil {
		c.Close()
		return err
	}
	p.client = c
	p.connected.Store(true)
	p.reconnects.Add(1)
	p.log.Info("connected")
	return nil
}

func (p *Poller) disconnect() {
	if p.client != nil {
		p.client.Close()
		p.client = nil
	}
	p.connected.Store(false)
}

// recordErr logs an error once per distinct message, so a dead device does
// not flood the log every poll.
func (p *Poller) recordErr(err error) {
	p.mu.Lock()
	first := p.lastErr != err.Error()
	p.lastErr = err.Error()
	p.mu.Unlock()
	if first {
		p.log.Warn("device error", "err", err)
	}
}

// pollOnce reads every block. A Modbus exception on one block only affects
// that block's tags; a transport error drops the connection, because a late
// reply after a timeout would desynchronise transaction IDs.
func (p *Poller) pollOnce() error {
	start := time.Now()
	p.polls.Add(1)
	var firstErr error
	hadErr := false
	for _, b := range p.blocks {
		rt := modbus.HOLDING_REGISTER
		if b.Kind == config.KindInput {
			rt = modbus.INPUT_REGISTER
		}
		regs, err := p.client.ReadRegisters(b.Start, b.Count, rt)
		now := time.Now()
		if err != nil {
			p.pollErrors.Add(1)
			hadErr = true
			class := classify(err)
			q := tagstore.CommFailure
			if class == errConfig {
				q = tagstore.ConfigError
			}
			p.recordErr(fmt.Errorf("read %s %d+%d: %w", b.Kind, b.Start, b.Count, err))
			switch class {
			case errTransport:
				p.store.SetQuality(p.dev.Name+".", tagstore.CommFailure)
				p.disconnect()
				return err
			case errDevice:
				// Device is busy or failing: back off, keep the link.
				if firstErr == nil {
					firstErr = err
				}
			}
			// errConfig is permanent until the register map is fixed; keep
			// polling the other blocks at the normal rate.
			p.markBlock(b, q, now)
			continue
		}
		for _, ti := range b.Tags {
			t := p.dev.Tags[ti]
			off := t.Address - b.Start
			w, _ := config.Width(t.Type)
			v, err := Decode(regs[off:off+w], t, p.dev.WordOrder)
			key := config.Key(p.dev.Name, t.Name)
			if err != nil {
				old, _ := p.store.Get(key)
				p.store.Set(key, tagstore.Value{V: old.V, Q: tagstore.ConfigError, TS: now})
				continue
			}
			p.store.Set(key, tagstore.Value{V: v, Q: tagstore.Good, TS: now})
		}
	}
	p.mu.Lock()
	p.lastPoll = time.Now()
	p.lastPollDuration = time.Since(start)
	if !hadErr {
		p.lastErr = ""
	}
	p.mu.Unlock()
	return firstErr
}

func (p *Poller) markBlock(b Block, q tagstore.Quality, now time.Time) {
	for _, ti := range b.Tags {
		key := config.Key(p.dev.Name, p.dev.Tags[ti].Name)
		old, _ := p.store.Get(key)
		p.store.Set(key, tagstore.Value{V: old.V, Q: q, TS: now})
	}
}

// Write sends a setpoint to the device and waits for the result.
func (p *Poller) Write(ctx context.Context, tag string, value float64) error {
	ti, ok := p.byName[tag]
	if !ok {
		return fmt.Errorf("unknown tag %s.%s", p.dev.Name, tag)
	}
	if !p.dev.Tags[ti].Writable {
		return fmt.Errorf("tag %s.%s is not writable", p.dev.Name, tag)
	}
	req := writeReq{tag: ti, value: value, res: make(chan error, 1)}
	select {
	case p.writes <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.res:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Poller) doWrite(w writeReq) error {
	t := p.dev.Tags[w.tag]
	if p.client == nil {
		p.writeErrors.Add(1)
		return ErrNotConnected
	}
	regs, err := Encode(w.value, t, p.dev.WordOrder)
	if err != nil {
		p.writeErrors.Add(1)
		return err
	}
	if err := p.client.WriteRegisters(t.Address, regs); err != nil {
		p.writeErrors.Add(1)
		if classify(err) == errTransport {
			p.disconnect()
		}
		return fmt.Errorf("write %s: %w", t.Name, err)
	}
	p.writesOK.Add(1)
	p.log.Info("setpoint written", "tag", t.Name, "value", w.value)
	return nil
}

// Status is a point-in-time view of the poller for the status endpoint.
type Status struct {
	Device           string    `json:"device"`
	Address          string    `json:"address"`
	Connected        bool      `json:"connected"`
	Polls            int64     `json:"polls"`
	PollErrors       int64     `json:"poll_errors"`
	Reconnects       int64     `json:"connects"`
	Writes           int64     `json:"writes"`
	WriteErrors      int64     `json:"write_errors"`
	LastError        string    `json:"last_error,omitempty"`
	LastPoll         time.Time `json:"last_poll"`
	LastPollDuration string    `json:"last_poll_duration"`
}

// Status returns current counters.
func (p *Poller) Status() Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Status{
		Device:           p.dev.Name,
		Address:          p.dev.Address,
		Connected:        p.connected.Load(),
		Polls:            p.polls.Load(),
		PollErrors:       p.pollErrors.Load(),
		Reconnects:       p.reconnects.Load(),
		Writes:           p.writesOK.Load(),
		WriteErrors:      p.writeErrors.Load(),
		LastError:        p.lastErr,
		LastPoll:         p.lastPoll,
		LastPollDuration: p.lastPollDuration.String(),
	}
}

type errClass int

const (
	errTransport errClass = iota // drop and reconnect
	errDevice                    // device answered with a failure; link is fine
	errConfig                    // device rejected the request; fix the register map
)

func classify(err error) errClass {
	var me modbus.Error
	if errors.As(err, &me) {
		switch me {
		case modbus.ErrIllegalFunction, modbus.ErrIllegalDataAddress, modbus.ErrIllegalDataValue:
			return errConfig
		case modbus.ErrServerDeviceFailure, modbus.ErrServerDeviceBusy, modbus.ErrAcknowledge,
			modbus.ErrGWPathUnavailable, modbus.ErrGWTargetFailedToRespond:
			return errDevice
		}
	}
	return errTransport
}
