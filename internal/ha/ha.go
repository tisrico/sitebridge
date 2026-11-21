// Package ha implements active/standby redundancy between two sitebridge
// nodes using UDP heartbeats.
//
// Only the active node polls devices and accepts setpoints, so each device has
// exactly one Modbus master. The standby takes over when it stops hearing the
// active node for FailoverTimeout. Failover is non-revertive: a recovered node
// rejoins as standby even if it has higher priority, so the site does not
// switch twice for one fault.
//
// Limitation: with two nodes and no third party there is no quorum. If the
// link between the nodes fails while both can still reach the devices, both
// become active (split brain). When the link heals, the node with the lower
// term (the one that took over less recently, or lost the tie-break) steps
// down. A production site needs a fencing path, for example a witness on the
// device network or a hardware interlock; see README.
package ha

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Role is a node's role in the pair.
type Role int

const (
	Standby Role = iota
	Active
)

func (r Role) String() string {
	if r == Active {
		return "active"
	}
	return "standby"
}

// MarshalJSON encodes the role as a string.
func (r Role) MarshalJSON() ([]byte, error) { return json.Marshal(r.String()) }

// UnmarshalJSON decodes a role string.
func (r *Role) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*r = Standby
	if s == "active" {
		*r = Active
	}
	return nil
}

// Heartbeat is the message each node sends to its peer.
type Heartbeat struct {
	Node     string `json:"node"`
	Priority int    `json:"priority"`
	Role     Role   `json:"role"`
	// Term increases every time a node becomes active. When both nodes
	// claim active, the higher term is the more recent takeover and wins.
	Term uint64 `json:"term"`
	// Seq starts at 1 when a node starts; a reset to 1 means it restarted.
	Seq uint64 `json:"seq"`
	// Leaving is set in the final heartbeat of a clean shutdown so the
	// peer takes over at once instead of waiting for the timeout.
	Leaving bool `json:"leaving,omitempty"`
}

// Config configures an Elector.
type Config struct {
	NodeID            string
	Priority          int
	Listen            string
	Peer              string
	HeartbeatInterval time.Duration
	FailoverTimeout   time.Duration
}

// state is everything the decision depends on.
type state struct {
	self     Heartbeat
	started  time.Time
	peer     *Heartbeat
	peerSeen time.Time
}

// outranks reports whether a should be preferred over b when neither is
// already active: higher priority, then lower node ID.
func outranks(a, b Heartbeat) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return a.Node < b.Node
}

// keepsActive decides a dual-active conflict: higher term, then rank.
func keepsActive(a, b Heartbeat) bool {
	if a.Term != b.Term {
		return a.Term > b.Term
	}
	return outranks(a, b)
}

// decide returns the role this node should have now. It is a pure function
// of the state so every transition can be unit tested.
func decide(s *state, now time.Time, timeout time.Duration) Role {
	peerAlive := s.peer != nil && !s.peer.Leaving && now.Sub(s.peerSeen) < timeout
	switch s.self.Role {
	case Standby:
		if !peerAlive {
			// If we have never heard the peer, wait one full timeout after
			// start-up for an existing active node before claiming the site.
			if s.peer != nil || now.Sub(s.started) >= timeout {
				return Active
			}
			return Standby
		}
		if s.peer.Role == Standby && outranks(s.self, *s.peer) {
			return Active // both standby: the better-ranked node takes over
		}
		return Standby
	default: // Active
		if peerAlive && s.peer.Role == Active && !keepsActive(s.self, *s.peer) {
			return Standby // split brain resolved in the peer's favour
		}
		return Active
	}
}

// Elector runs the heartbeat protocol and reports role changes.
type Elector struct {
	cfg  Config
	log  *slog.Logger
	conn *net.UDPConn
	peer *net.UDPAddr
	now  func() time.Time

	mu    sync.Mutex
	st    state
	roles chan Role
	wake  chan struct{}
}

// New creates an elector and binds its UDP socket.
func New(cfg Config, log *slog.Logger) (*Elector, error) {
	laddr, err := net.ResolveUDPAddr("udp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	paddr, err := net.ResolveUDPAddr("udp", cfg.Peer)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}
	e := &Elector{
		cfg: cfg, log: log, conn: conn, peer: paddr, now: time.Now,
		roles: make(chan Role, 8),
		wake:  make(chan struct{}, 1),
	}
	e.st = state{self: Heartbeat{Node: cfg.NodeID, Priority: cfg.Priority, Role: Standby}, started: e.now()}
	return e, nil
}

// Roles delivers every role change, starting from Standby.
func (e *Elector) Roles() <-chan Role { return e.roles }

// Status is the elector's view for the status endpoint.
type Status struct {
	Role         Role       `json:"role"`
	Term         uint64     `json:"term"`
	Peer         *Heartbeat `json:"peer,omitempty"`
	PeerLastSeen time.Time  `json:"peer_last_seen,omitempty"`
}

// Status returns the current state.
func (e *Elector) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := Status{Role: e.st.self.Role, Term: e.st.self.Term, PeerLastSeen: e.st.peerSeen}
	if e.st.peer != nil {
		p := *e.st.peer
		s.Peer = &p
	}
	return s
}

// Run sends heartbeats and evaluates the role until ctx is cancelled. On exit
// it sends a final standby heartbeat so the peer can take over immediately
// instead of waiting for the timeout.
func (e *Elector) Run(ctx context.Context) error {
	defer e.conn.Close()
	go e.receive(ctx)

	// All decisions are made on this goroutine, so role changes are
	// delivered in the order they were decided.
	t := time.NewTicker(e.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		e.tick()
		select {
		case <-ctx.Done():
			e.mu.Lock()
			e.st.self.Role = Standby
			e.st.self.Leaving = true
			e.mu.Unlock()
			e.send()
			return nil
		case <-t.C:
		case <-e.wake:
		}
	}
}

func (e *Elector) tick() {
	e.mu.Lock()
	prev := e.st.self.Role
	next := decide(&e.st, e.now(), e.cfg.FailoverTimeout)
	if next != prev {
		e.st.self.Role = next
		if next == Active {
			peerTerm := uint64(0)
			if e.st.peer != nil {
				peerTerm = e.st.peer.Term
			}
			e.st.self.Term = max(e.st.self.Term, peerTerm) + 1
		}
		e.log.Warn("role change", "from", prev, "to", next, "term", e.st.self.Term, "peer", e.peerString())
	}
	e.mu.Unlock()

	if next != prev {
		e.roles <- next
	}
	e.send()
}

func (e *Elector) peerString() string {
	if e.st.peer == nil {
		return "never seen"
	}
	return e.st.peer.Node + "/" + e.st.peer.Role.String()
}

func (e *Elector) send() {
	e.mu.Lock()
	e.st.self.Seq++
	b, _ := json.Marshal(e.st.self)
	e.mu.Unlock()
	if _, err := e.conn.WriteToUDP(b, e.peer); err != nil {
		e.log.Debug("heartbeat send failed", "err", err)
	}
}

func (e *Elector) receive(ctx context.Context) {
	buf := make([]byte, 1024)
	for {
		e.conn.SetReadDeadline(time.Now().Add(e.cfg.HeartbeatInterval))
		n, _, err := e.conn.ReadFromUDP(buf)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		var hb Heartbeat
		if err := json.Unmarshal(buf[:n], &hb); err != nil || hb.Node == "" {
			continue
		}
		if hb.Node == e.cfg.NodeID {
			e.log.Error("received heartbeat with our own node id; check ha.peer and node.id")
			continue
		}
		e.mu.Lock()
		var changed bool
		if e.st.peer == nil || e.st.peer.Node != hb.Node || hb.Seq > e.st.peer.Seq || hb.Seq == 1 {
			// Drop reordered (stale) datagrams; Seq 1 means the peer restarted.
			changed = e.st.peer == nil || e.st.peer.Role != hb.Role || hb.Leaving
			e.st.peer = &hb
			e.st.peerSeen = e.now()
		}
		e.mu.Unlock()

		// React to the peer changing role (split brain, stepping down,
		// leaving) without waiting for the next tick.
		if changed {
			select {
			case e.wake <- struct{}{}:
			default:
			}
		}
	}
}
