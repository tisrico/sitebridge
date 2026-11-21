package modbusdev

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/tisrico/sitebridge/internal/config"
	"github.com/tisrico/sitebridge/internal/sim"
	"github.com/tisrico/sitebridge/internal/tagstore"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func batteryDevice(addr string) config.Device {
	return config.Device{
		Name: "bess1", Address: "tcp://" + addr, UnitID: 1,
		PollInterval: 20 * time.Millisecond, Timeout: 200 * time.Millisecond,
		WordOrder: config.HighWordFirst, MaxGap: 4,
		Tags: []config.Tag{
			{Name: "soc", Kind: config.KindInput, Address: sim.BatterySoC, Type: "uint16", Scale: 0.1},
			{Name: "power", Kind: config.KindInput, Address: sim.BatteryPower, Type: "int16", Scale: 1},
			{Name: "energy", Kind: config.KindInput, Address: sim.BatteryEnergy, Type: "uint32", Scale: 1},
			{Name: "setpoint", Kind: config.KindHolding, Address: sim.BatterySetpoint, Type: "int16", Scale: 1, Writable: true},
			{Name: "missing", Kind: config.KindHolding, Address: 500, Type: "uint16", Scale: 1},
		},
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPollerReadWriteAndRecover(t *testing.T) {
	addr := freeAddr(t)
	bat := sim.NewBattery(80)
	srv, err := sim.Serve(addr, bat)
	if err != nil {
		t.Fatal(err)
	}

	dev := batteryDevice(addr)
	store := tagstore.New([]string{"bess1.soc", "bess1.power", "bess1.energy", "bess1.setpoint", "bess1.missing"})
	p := NewPoller(dev, store, TCPDialer, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()

	waitFor(t, "good soc", func() bool {
		v, _ := store.Get("bess1.soc")
		return v.Q == tagstore.Good && v.V > 79 && v.V < 81
	})

	// A bad register address only affects its own tag.
	waitFor(t, "config error on missing tag", func() bool {
		v, _ := store.Get("bess1.missing")
		return v.Q == tagstore.ConfigError
	})
	if v, _ := store.Get("bess1.soc"); v.Q != tagstore.Good {
		t.Fatalf("soc quality affected by unrelated bad tag: %v", v.Q)
	}

	// Setpoint write goes through the poller's single connection.
	wctx, wcancel := context.WithTimeout(ctx, time.Second)
	if err := p.Write(wctx, "setpoint", -120); err != nil {
		t.Fatal(err)
	}
	wcancel()
	if got := bat.Setpoint(); got != -120 {
		t.Fatalf("simulator setpoint = %v, want -120", got)
	}
	if err := p.Write(ctx, "soc", 1); err == nil {
		t.Fatal("expected error writing read-only tag")
	}

	// Device goes away: tags keep last value but go to CommFailure.
	srv.Stop()
	waitFor(t, "comm failure", func() bool {
		v, _ := store.Get("bess1.soc")
		return v.Q == tagstore.CommFailure && v.V > 79
	})

	// Device comes back: poller reconnects on its own.
	srv, err = sim.Serve(addr, bat)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()
	waitFor(t, "recovery", func() bool {
		v, _ := store.Get("bess1.soc")
		return v.Q == tagstore.Good
	})
	if s := p.Status(); s.Reconnects < 2 {
		t.Errorf("expected at least 2 connects, got %d", s.Reconnects)
	}

	cancel()
	<-done
	if v, _ := store.Get("bess1.soc"); v.Q != tagstore.OutOfService {
		t.Errorf("after stop quality = %v, want out_of_service", v.Q)
	}
}

func TestWriteWhileDisconnected(t *testing.T) {
	dev := batteryDevice(freeAddr(t)) // nothing listening
	store := tagstore.New(nil)
	p := NewPoller(dev, store, TCPDialer, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	wctx, wcancel := context.WithTimeout(ctx, 2*time.Second)
	defer wcancel()
	if err := p.Write(wctx, "setpoint", 10); err == nil {
		t.Fatal("expected error writing while disconnected")
	}
}
