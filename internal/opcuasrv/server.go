// Package opcuasrv exposes the site's devices as a consistent OPC UA address
// space, whatever protocol each device speaks underneath.
//
// Address space (namespace configured by opcua.namespace):
//
//	Objects/<namespace>/
//	  sitebridge.role             String  "active" | "standby"
//	  <device>/
//	    <device>.<tag>            Double  read-only, live value with quality
//	    <device>.<tag>.setpoint   Double  writable (only for writable tags)
//
// Device quality maps onto OPC UA status codes, so a client can tell a live
// value from a last-known one: Good, BadCommunicationError,
// BadConfigurationError, BadOutOfService, BadWaitingForInitialData.
package opcuasrv

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/server"
	"github.com/gopcua/opcua/ua"

	"github.com/tisrico/sitebridge/internal/config"
	"github.com/tisrico/sitebridge/internal/tagstore"
)

// Writer applies a setpoint to a device.
type Writer interface {
	Write(ctx context.Context, device, tag string, value float64) error
}

type setpoint struct {
	device, tag string
	node        *server.Node
}

type desired struct {
	device  string
	tag     string
	value   float64
	version uint64
	applied uint64
	lastErr string
}

// Server is the OPC UA server.
type Server struct {
	srv    *server.Server
	ns     *server.NodeNameSpace
	store  *tagstore.Store
	log    *slog.Logger
	active atomic.Bool

	nodeIDs   map[string]*ua.NodeID // store key -> node
	setpoints map[string]*setpoint  // node id string -> setpoint
	roleNode  *ua.NodeID

	mu      sync.Mutex
	desired map[string]*desired // store key -> latest requested value
	nudge   chan struct{}
}

// New builds the server and its address space. Call Start to listen.
func New(cfg config.OPCUA, devices []config.Device, store *tagstore.Store, log *slog.Logger) *Server {
	s := &Server{
		store:     store,
		log:       log,
		nodeIDs:   map[string]*ua.NodeID{},
		setpoints: map[string]*setpoint{},
		desired:   map[string]*desired{},
		nudge:     make(chan struct{}, 1),
	}
	s.srv = server.New(
		server.EnableSecurity("None", ua.MessageSecurityModeNone),
		server.EnableAuthMode(ua.UserTokenTypeAnonymous),
		server.EndPoint(cfg.Host, cfg.Port),
		server.ServerName("sitebridge"),
		server.ProductName("sitebridge"),
		server.SetLogger(logAdapter{log}),
	)

	s.ns = server.NewNodeNameSpace(s.srv, cfg.Namespace)
	// The library delivers client writes on an unbuffered channel with a
	// non-blocking send, which drops notifications if the reader is busy.
	// A buffer makes that loss practically impossible.
	s.ns.ExternalNotification = make(chan *ua.NodeID, 1024)

	root, _ := s.srv.Namespace(0)
	root.Objects().AddRef(s.ns.Objects(), id.HasComponent, true)

	s.roleNode = ua.NewStringNodeID(s.ns.ID(), "sitebridge.role")
	role := server.NewVariableNode(s.roleNode, "role", func() *ua.DataValue {
		r := "standby"
		if s.active.Load() {
			r = "active"
		}
		return &ua.DataValue{
			EncodingMask: ua.DataValueValue | ua.DataValueServerTimestamp,
			Value:        ua.MustVariant(r), ServerTimestamp: time.Now(),
		}
	})
	setReadOnly(role, id.String)
	s.ns.AddNode(role)
	s.ns.Objects().AddRef(role, id.HasComponent, true)

	for _, d := range devices {
		folder := server.NewFolderNode(ua.NewStringNodeID(s.ns.ID(), d.Name), d.Name)
		s.ns.AddNode(folder)
		s.ns.Objects().AddRef(folder, id.HasComponent, true)

		for _, t := range d.Tags {
			key := config.Key(d.Name, t.Name)
			nid := ua.NewStringNodeID(s.ns.ID(), key)
			s.nodeIDs[key] = nid
			n := server.NewVariableNode(nid, t.Name, func() *ua.DataValue { return s.dataValue(key) })
			setReadOnly(n, id.Double)
			if t.Unit != "" {
				n.SetDescription(t.Unit, "")
			}
			s.ns.AddNode(n)
			folder.AddRef(n, id.HasComponent, true)

			if t.Writable {
				spID := ua.NewStringNodeID(s.ns.ID(), key+".setpoint")
				initial := &ua.DataValue{
					EncodingMask: ua.DataValueValue | ua.DataValueStatusCode,
					Value:        ua.MustVariant(float64(0)),
					Status:       ua.StatusBadWaitingForInitialData,
				}
				// A client write replaces this value function with the
				// written value.
				sp := server.NewVariableNode(spID, t.Name+".setpoint", func() *ua.DataValue { return initial })
				setAccess(sp, id.Double, ua.AccessLevelTypeCurrentRead|ua.AccessLevelTypeCurrentWrite)
				s.ns.AddNode(sp)
				folder.AddRef(sp, id.HasComponent, true)
				s.setpoints[spID.String()] = &setpoint{device: d.Name, tag: t.Name, node: sp}
			}
		}
	}

	store.OnChange(func(key string) {
		if nid, ok := s.nodeIDs[key]; ok {
			s.srv.ChangeNotification(nid)
		}
	})
	return s
}

func setReadOnly(n *server.Node, dataType uint32) {
	setAccess(n, dataType, ua.AccessLevelTypeCurrentRead)
}

func setAccess(n *server.Node, dataType uint32, level ua.AccessLevelType) {
	n.SetAttribute(ua.AttributeIDDataType, server.DataValueFromValue(ua.NewNumericNodeID(0, dataType)))
	n.SetAttribute(ua.AttributeIDAccessLevel, server.DataValueFromValue(uint8(level)))
	n.SetAttribute(ua.AttributeIDUserAccessLevel, server.DataValueFromValue(uint8(level)))
}

func (s *Server) dataValue(key string) *ua.DataValue {
	v, _ := s.store.Get(key)
	dv := &ua.DataValue{
		EncodingMask:    ua.DataValueValue | ua.DataValueStatusCode | ua.DataValueServerTimestamp,
		Value:           ua.MustVariant(v.V),
		Status:          statusFor(v.Q),
		ServerTimestamp: time.Now(),
	}
	if !v.TS.IsZero() {
		dv.EncodingMask |= ua.DataValueSourceTimestamp
		dv.SourceTimestamp = v.TS
	}
	return dv
}

func statusFor(q tagstore.Quality) ua.StatusCode {
	switch q {
	case tagstore.Good:
		return ua.StatusOK
	case tagstore.CommFailure:
		return ua.StatusBadCommunicationError
	case tagstore.ConfigError:
		return ua.StatusBadConfigurationError
	case tagstore.OutOfService:
		return ua.StatusBadOutOfService
	}
	return ua.StatusBadWaitingForInitialData
}

// Start begins listening and handling client writes.
func (s *Server) Start(ctx context.Context) error {
	if err := s.srv.Start(ctx); err != nil {
		return err
	}
	go s.receiveWrites(ctx)
	return nil
}

// Close stops the server.
func (s *Server) Close() error { return s.srv.Close() }

// URLs returns the endpoint URLs.
func (s *Server) URLs() []string { return s.srv.URLs() }

// SetActive switches setpoint handling on or off. On becoming active any
// setpoints written while standby are discarded rather than replayed, so a
// failover can never apply a stale command.
func (s *Server) SetActive(active bool) {
	s.mu.Lock()
	s.desired = map[string]*desired{}
	s.mu.Unlock()
	s.active.Store(active)
	s.srv.ChangeNotification(s.roleNode)
}

// receiveWrites turns client writes into desired setpoints. Reading the node
// right after the notification is safe: the library writes the node before
// sending on the channel.
func (s *Server) receiveWrites(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case nid := <-s.ns.ExternalNotification:
			sp, ok := s.setpoints[nid.String()]
			if !ok {
				continue
			}
			dv := sp.node.Value()
			v, err := toFloat(dv)
			if err != nil {
				s.log.Warn("rejected setpoint", "node", nid.String(), "err", err)
				continue
			}
			if !s.active.Load() {
				s.log.Warn("setpoint ignored: this node is standby", "node", nid.String(), "value", v)
				continue
			}
			key := config.Key(sp.device, sp.tag)
			s.mu.Lock()
			d := s.desired[key]
			if d == nil {
				d = &desired{device: sp.device, tag: sp.tag}
				s.desired[key] = d
			}
			d.value = v
			d.version++
			s.mu.Unlock()
			select {
			case s.nudge <- struct{}{}:
			default:
			}
		}
	}
}

// RunSetpoints applies desired setpoints to devices until ctx is cancelled.
// It is level-triggered: only the latest value per tag is applied, and a
// failed write is retried until it succeeds or is superseded. Run it only
// while active.
func (s *Server) RunSetpoints(ctx context.Context, w Writer) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.nudge:
		case <-t.C:
		}
		s.mu.Lock()
		type job struct {
			key string
			d   desired
		}
		var jobs []job
		for k, d := range s.desired {
			if d.version != d.applied {
				jobs = append(jobs, job{k, *d})
			}
		}
		s.mu.Unlock()

		for _, j := range jobs {
			wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := w.Write(wctx, j.d.device, j.d.tag, j.d.value)
			cancel()
			s.mu.Lock()
			if d := s.desired[j.key]; d != nil {
				if err == nil {
					d.applied = j.d.version
					d.lastErr = ""
				} else if d.lastErr != err.Error() {
					d.lastErr = err.Error()
					s.log.Warn("setpoint write failed; will retry", "tag", j.key, "value", j.d.value, "err", err)
				}
			}
			s.mu.Unlock()
		}
	}
}

func toFloat(dv *ua.DataValue) (float64, error) {
	if dv == nil || dv.Value == nil {
		return 0, fmt.Errorf("no value")
	}
	switch v := dv.Value.Value().(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	}
	return 0, fmt.Errorf("unsupported type %T", dv.Value.Value())
}

type logAdapter struct{ l *slog.Logger }

func (a logAdapter) Debug(msg string, args ...any) { a.l.Debug(fmt.Sprintf(msg, args...)) }
func (a logAdapter) Info(msg string, args ...any)  { a.l.Debug(fmt.Sprintf(msg, args...)) }
func (a logAdapter) Warn(msg string, args ...any)  { a.l.Warn(fmt.Sprintf(msg, args...)) }
func (a logAdapter) Error(msg string, args ...any) { a.l.Error(fmt.Sprintf(msg, args...)) }
