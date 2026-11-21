package opcuasrv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"

	"github.com/tisrico/sitebridge/internal/config"
	"github.com/tisrico/sitebridge/internal/tagstore"
)

type fakeWriter struct {
	mu     sync.Mutex
	writes []string
	fail   int // fail this many writes first
}

func (f *fakeWriter) Write(_ context.Context, dev, tag string, v float64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail > 0 {
		f.fail--
		return fmt.Errorf("device busy")
	}
	f.writes = append(f.writes, fmt.Sprintf("%s.%s=%v", dev, tag, v))
	return nil
}

func (f *fakeWriter) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.writes...)
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func setup(t *testing.T) (*Server, *tagstore.Store, *opcua.Client, context.Context) {
	t.Helper()
	port := freePort(t)
	devs := []config.Device{{Name: "bess1", Tags: []config.Tag{
		{Name: "soc", Type: "uint16", Scale: 0.1, Unit: "%"},
		{Name: "power_setpoint", Type: "int16", Scale: 1, Writable: true},
	}}}
	store := tagstore.New([]string{"bess1.soc", "bess1.power_setpoint"})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(config.OPCUA{Host: "127.0.0.1", Port: port, Namespace: "urn:test"}, devs, store, log)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	c, err := opcua.NewClient(fmt.Sprintf("opc.tcp://127.0.0.1:%d", port), opcua.SecurityMode(ua.MessageSecurityModeNone))
	if err != nil {
		t.Fatal(err)
	}
	cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
	defer ccancel()
	if err := c.Connect(cctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return s, store, c, ctx
}

func read(t *testing.T, ctx context.Context, c *opcua.Client, nodeID string) *ua.DataValue {
	t.Helper()
	id := ua.MustParseNodeID(nodeID)
	resp, err := c.Read(ctx, &ua.ReadRequest{NodesToRead: []*ua.ReadValueID{{NodeID: id, AttributeID: ua.AttributeIDValue}}})
	if err != nil {
		t.Fatal(err)
	}
	return resp.Results[0]
}

func write(t *testing.T, ctx context.Context, c *opcua.Client, nodeID string, v float64) ua.StatusCode {
	t.Helper()
	resp, err := c.Write(ctx, &ua.WriteRequest{NodesToWrite: []*ua.WriteValue{{
		NodeID: ua.MustParseNodeID(nodeID), AttributeID: ua.AttributeIDValue,
		Value: &ua.DataValue{EncodingMask: ua.DataValueValue, Value: ua.MustVariant(v)},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return resp.Results[0]
}

func TestValuesAndQuality(t *testing.T) {
	_, store, c, ctx := setup(t)

	if dv := read(t, ctx, c, "ns=1;s=bess1.soc"); dv.Status != ua.StatusBadWaitingForInitialData {
		t.Fatalf("initial status = %v", dv.Status)
	}
	store.Set("bess1.soc", tagstore.Value{V: 81.5, Q: tagstore.Good, TS: time.Now()})
	dv := read(t, ctx, c, "ns=1;s=bess1.soc")
	if dv.Status != ua.StatusOK || dv.Value.Value().(float64) != 81.5 {
		t.Fatalf("good read: %v %v", dv.Status, dv.Value.Value())
	}
	store.SetQuality("bess1.", tagstore.CommFailure)
	dv = read(t, ctx, c, "ns=1;s=bess1.soc")
	if dv.Status != ua.StatusBadCommunicationError || dv.Value.Value().(float64) != 81.5 {
		t.Fatalf("comm failure read: %v %v", dv.Status, dv.Value.Value())
	}
	if st := write(t, ctx, c, "ns=1;s=bess1.soc", 1); st == ua.StatusOK {
		t.Fatal("write to read-only tag succeeded")
	}
}

func TestSetpointsOnlyWhenActive(t *testing.T) {
	s, _, c, ctx := setup(t)
	w := &fakeWriter{fail: 2}

	// Standby: write is accepted by OPC UA but never reaches the device.
	if st := write(t, ctx, c, "ns=1;s=bess1.power_setpoint.setpoint", -50); st != ua.StatusOK {
		t.Fatalf("write status %v", st)
	}
	if r := read(t, ctx, c, "ns=1;s=sitebridge.role"); r.Value.Value().(string) != "standby" {
		t.Fatalf("role = %v", r.Value.Value())
	}

	// Become active: the standby-era write must not be replayed.
	s.SetActive(true)
	actx, acancel := context.WithCancel(ctx)
	defer acancel()
	go s.RunSetpoints(actx, w)
	time.Sleep(200 * time.Millisecond)
	if got := w.got(); len(got) != 0 {
		t.Fatalf("stale standby setpoint replayed: %v", got)
	}

	// Active: two failures, then the latest value is applied once.
	write(t, ctx, c, "ns=1;s=bess1.power_setpoint.setpoint", 120)
	deadline := time.Now().Add(5 * time.Second)
	for len(w.got()) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := w.got(); len(got) != 1 || got[0] != "bess1.power_setpoint=120" {
		t.Fatalf("writes = %v", got)
	}
}
