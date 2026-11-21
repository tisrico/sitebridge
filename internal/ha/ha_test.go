package ha

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

const timeout = time.Second

func TestDecide(t *testing.T) {
	t0 := time.Unix(1000, 0)
	hb := func(node string, prio int, role Role, term uint64) *Heartbeat {
		return &Heartbeat{Node: node, Priority: prio, Role: role, Term: term}
	}
	cases := []struct {
		name    string
		self    Heartbeat
		peer    *Heartbeat
		peerAge time.Duration
		uptime  time.Duration
		want    Role
	}{
		{"start-up hold: no peer yet", *hb("a", 100, Standby, 0), nil, 0, 500 * time.Millisecond, Standby},
		{"no peer after hold: take over", *hb("a", 100, Standby, 0), nil, 0, 2 * time.Second, Active},
		{"peer active: stay standby", *hb("a", 100, Standby, 0), hb("b", 50, Active, 3), 100 * time.Millisecond, 5 * time.Second, Standby},
		{"peer active but silent: take over", *hb("a", 50, Standby, 0), hb("b", 100, Active, 3), 1500 * time.Millisecond, 5 * time.Second, Active},
		{"both standby, we outrank", *hb("a", 100, Standby, 0), hb("b", 50, Standby, 0), 0, 0, Active},
		{"both standby, peer outranks", *hb("a", 50, Standby, 0), hb("b", 100, Standby, 0), 0, 0, Standby},
		{"both standby, equal priority, lower id wins", *hb("a", 100, Standby, 0), hb("b", 100, Standby, 0), 0, 0, Active},
		{"non-revertive: active keeps role vs higher-priority standby", *hb("b", 50, Active, 2), hb("a", 100, Standby, 0), 0, time.Hour, Active},
		{"split brain: lower term steps down", *hb("a", 100, Active, 2), hb("b", 50, Active, 3), 0, time.Hour, Standby},
		{"split brain: higher term stays", *hb("b", 50, Active, 3), hb("a", 100, Active, 2), 0, time.Hour, Active},
		{"split brain: equal term, rank decides", *hb("a", 100, Active, 3), hb("b", 50, Active, 3), 0, time.Hour, Active},
		{"peer leaving during start-up hold: take over", *hb("a", 50, Standby, 0), &Heartbeat{Node: "b", Priority: 100, Role: Standby, Leaving: true}, 0, 10 * time.Millisecond, Active},
		{"peer leaving: take over immediately", *hb("a", 50, Standby, 0), &Heartbeat{Node: "b", Priority: 100, Role: Standby, Leaving: true}, 0, time.Hour, Active},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			now := t0.Add(time.Hour)
			s := &state{self: c.self, started: now.Add(-c.uptime), peer: c.peer, peerSeen: now.Add(-c.peerAge)}
			if got := decide(s, now, timeout); got != c.want {
				t.Errorf("got %v want %v", got, c.want)
			}
		})
	}
}

func freeUDP(t *testing.T) string {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().String()
}

type node struct {
	e      *Elector
	cancel context.CancelFunc
	done   chan struct{}
}

func start(t *testing.T, id string, prio int, listen, peer string) *node {
	t.Helper()
	e, err := New(Config{
		NodeID: id, Priority: prio, Listen: listen, Peer: peer,
		HeartbeatInterval: 20 * time.Millisecond, FailoverTimeout: 150 * time.Millisecond,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{e: e, cancel: cancel, done: make(chan struct{})}
	go func() {
		go func() {
			for range e.Roles() {
			}
		}()
		e.Run(ctx)
		close(n.done)
	}()
	return n
}

func (n *node) stop() { n.cancel(); <-n.done }

func waitRole(t *testing.T, n *node, want Role) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < 3*time.Second {
		if n.e.Status().Role == want {
			return time.Since(start)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s did not become %v", n.e.cfg.NodeID, want)
	return 0
}

func TestPairFailoverOverUDP(t *testing.T) {
	addrA, addrB := freeUDP(t), freeUDP(t)
	a := start(t, "a", 100, addrA, addrB)
	b := start(t, "b", 50, addrB, addrA)
	defer b.stop()

	waitRole(t, a, Active)
	time.Sleep(100 * time.Millisecond)
	if b.e.Status().Role != Standby {
		t.Fatal("b should be standby while a is active")
	}

	// Clean shutdown: a's final heartbeat says it is leaving, so b takes
	// over without waiting for the timeout. (Crash case: TestPairCrashFailover.)
	a.stop()
	d := waitRole(t, b, Active)
	t.Logf("clean handover took %v", d)
	if d > 100*time.Millisecond {
		t.Errorf("clean handover should not wait for the failover timeout (took %v)", d)
	}

	// a comes back with higher priority but must not pre-empt b.
	a = start(t, "a", 100, addrA, addrB)
	defer a.stop()
	time.Sleep(400 * time.Millisecond)
	if a.e.Status().Role != Standby || b.e.Status().Role != Active {
		t.Fatalf("non-revertive failed: a=%v b=%v", a.e.Status().Role, b.e.Status().Role)
	}
}

func TestPairCrashFailover(t *testing.T) {
	addrA, addrB := freeUDP(t), freeUDP(t)
	// Peer "a" is simulated by a socket we control, so we can go silent.
	conn, err := net.ListenUDP("udp", mustUDP(t, addrA))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	b := start(t, "b", 50, addrB, addrA)
	defer b.stop()

	peer := mustUDP(t, addrB)
	stop := make(chan struct{})
	go func() {
		seq := uint64(0)
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			seq++
			b, _ := jsonHB(Heartbeat{Node: "a", Priority: 100, Role: Active, Term: 1, Seq: seq})
			conn.WriteToUDP(b, peer)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	if b.e.Status().Role != Standby {
		t.Fatal("b should stay standby while a heartbeats as active")
	}
	close(stop) // a "crashes": heartbeats stop
	d := waitRole(t, b, Active)
	t.Logf("crash failover took %v (timeout 150ms)", d)
	if d < 100*time.Millisecond {
		t.Errorf("failover faster than the timeout: %v", d)
	}
	if b.e.Status().Term != 2 {
		t.Errorf("new term = %d, want 2", b.e.Status().Term)
	}
}

func mustUDP(t *testing.T, s string) *net.UDPAddr {
	a, err := net.ResolveUDPAddr("udp", s)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func jsonHB(h Heartbeat) ([]byte, error) { return json.Marshal(h) }
