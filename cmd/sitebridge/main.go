// Command sitebridge is an edge bridge for an industrial energy site. It polls
// Modbus TCP field devices, exposes them through one OPC UA address space,
// buffers telemetry durably and delivers it to the cloud, and runs as one half
// of an active/standby pair.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/tisrico/sitebridge/internal/config"
	"github.com/tisrico/sitebridge/internal/ha"
	"github.com/tisrico/sitebridge/internal/modbusdev"
	"github.com/tisrico/sitebridge/internal/opcuasrv"
	"github.com/tisrico/sitebridge/internal/sdnotify"
	"github.com/tisrico/sitebridge/internal/tagstore"
	"github.com/tisrico/sitebridge/internal/telemetry"
)

var version = "dev"

func main() {
	cfgPath := flag.String("config", "/etc/sitebridge/sitebridge.yaml", "configuration file")
	debug := flag.Bool("debug", false, "debug logging")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log) // also captures libraries that use the standard log package

	if err := run(*cfgPath, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

type app struct {
	cfg      *config.Config
	log      *slog.Logger
	store    *tagstore.Store
	pollers  map[string]*modbusdev.Poller
	outbox   *telemetry.Outbox
	uploader *telemetry.Uploader
	opcua    *opcuasrv.Server
	elector  *ha.Elector
	started  time.Time

	mu   sync.Mutex
	role ha.Role
}

func run(cfgPath string, log *slog.Logger) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	log = log.With("node", cfg.Node.ID)
	a := &app{cfg: cfg, log: log, pollers: map[string]*modbusdev.Poller{}, started: time.Now()}

	var keys []string
	for _, d := range cfg.Devices {
		for _, t := range d.Tags {
			keys = append(keys, config.Key(d.Name, t.Name))
		}
	}
	a.store = tagstore.New(keys)
	for _, d := range cfg.Devices {
		a.pollers[d.Name] = modbusdev.NewPoller(d, a.store, modbusdev.TCPDialer, log)
	}

	a.outbox, err = telemetry.OpenOutbox(cfg.Telemetry.DB, cfg.Telemetry.Synchronous, cfg.Telemetry.MaxBacklog)
	if err != nil {
		return fmt.Errorf("open outbox: %w", err)
	}
	defer a.outbox.Close()
	log.Info("outbox open", "path", cfg.Telemetry.DB, "stream", a.outbox.StreamID(), "backlog", a.outbox.Len())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup

	if cfg.Telemetry.URL != "" {
		a.uploader = telemetry.NewUploader(telemetry.UploaderConfig{
			URL: cfg.Telemetry.URL, Source: cfg.Telemetry.Source,
			BatchMax: cfg.Telemetry.BatchMaxRecords, FlushInterval: cfg.Telemetry.FlushInterval,
			RequestTimeout: cfg.Telemetry.RequestTimeout,
			BackoffMin:     cfg.Telemetry.BackoffMin, BackoffMax: cfg.Telemetry.BackoffMax,
		}, a.outbox, log.With("component", "uploader"))
		// The uploader runs on both nodes: a standby still drains any
		// backlog it buffered while it was active.
		wg.Go(func() { a.uploader.Run(ctx) })
	}

	a.opcua = opcuasrv.New(cfg.OPCUA, cfg.Devices, a.store, log.With("component", "opcua"))
	if err := a.opcua.Start(ctx); err != nil {
		return fmt.Errorf("start opc ua: %w", err)
	}
	defer a.opcua.Close()
	log.Info("opc ua listening", "endpoints", a.opcua.URLs())

	httpSrv := &http.Server{Addr: cfg.HTTP.Listen, Handler: a.routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("status server", "err", err)
		}
	}()
	defer httpSrv.Close()

	roles := make(chan ha.Role, 1)
	if cfg.HA.Enabled {
		a.elector, err = ha.New(ha.Config{
			NodeID: cfg.Node.ID, Priority: cfg.Node.Priority,
			Listen: cfg.HA.Listen, Peer: cfg.HA.Peer,
			HeartbeatInterval: cfg.HA.HeartbeatInterval, FailoverTimeout: cfg.HA.FailoverTimeout,
		}, log.With("component", "ha"))
		if err != nil {
			return fmt.Errorf("start ha: %w", err)
		}
		electorDone := make(chan struct{})
		go func() { a.elector.Run(ctx); close(electorDone) }()
		defer func() { <-electorDone }()
		go func() {
			for r := range a.elector.Roles() {
				roles <- r
			}
		}()
		log.Info("ha enabled; starting as standby", "peer", cfg.HA.Peer, "failover_timeout", cfg.HA.FailoverTimeout)
	} else {
		roles <- ha.Active
	}

	sdnotify.Notify("READY=1")
	var watchdog <-chan time.Time
	if d := sdnotify.WatchdogInterval(); d > 0 {
		t := time.NewTicker(d)
		defer t.Stop()
		watchdog = t.C
	}

	// The main loop owns role transitions: active work runs under its own
	// context and is fully stopped before the node reports standby.
	var activeCancel context.CancelFunc
	var activeWG sync.WaitGroup
	stopActive := func() {
		if activeCancel != nil {
			a.opcua.SetActive(false)
			activeCancel()
			activeWG.Wait()
			activeCancel = nil
		}
	}
	defer stopActive()

	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down")
			stopActive()
			wg.Wait()
			return nil
		case <-watchdog:
			sdnotify.Notify("WATCHDOG=1")
		case r := <-roles:
			a.mu.Lock()
			a.role = r
			a.mu.Unlock()
			if r == ha.Active && activeCancel == nil {
				log.Warn("now ACTIVE: polling devices and accepting setpoints")
				actx, cancel := context.WithCancel(ctx)
				activeCancel = cancel
				a.opcua.SetActive(true)
				for _, p := range a.pollers {
					activeWG.Go(func() { p.Run(actx) })
				}
				activeWG.Go(func() { a.opcua.RunSetpoints(actx, a) })
				activeWG.Go(func() {
					telemetry.RunSampler(actx, cfg.Telemetry.SampleInterval, a.store, a.outbox, nil, log.With("component", "sampler"))
				})
			} else if r == ha.Standby {
				log.Warn("now STANDBY: device polling stopped")
				stopActive()
			}
		}
	}
}

// Write implements opcuasrv.Writer by routing to the device's poller.
func (a *app) Write(ctx context.Context, device, tag string, v float64) error {
	p, ok := a.pollers[device]
	if !ok {
		return fmt.Errorf("unknown device %s", device)
	}
	return p.Write(ctx, tag, v)
}

func (a *app) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		role := a.role
		a.mu.Unlock()
		st := map[string]any{
			"node":    a.cfg.Node.ID,
			"version": version,
			"role":    role,
			"uptime":  time.Since(a.started).Round(time.Second).String(),
		}
		if a.elector != nil {
			st["ha"] = a.elector.Status()
		}
		var devs []modbusdev.Status
		for _, d := range a.cfg.Devices {
			devs = append(devs, a.pollers[d.Name].Status())
		}
		st["devices"] = devs
		tel := map[string]any{
			"stream":  a.outbox.StreamID(),
			"backlog": a.outbox.Len(),
			"dropped": a.outbox.Dropped(),
		}
		if a.uploader != nil {
			tel["uploader"] = a.uploader.Stats()
		}
		st["telemetry"] = tel
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(st)
	})
	mux.HandleFunc("GET /tags", func(w http.ResponseWriter, r *http.Request) {
		type tv struct {
			V  float64   `json:"v"`
			Q  string    `json:"q"`
			TS time.Time `json:"ts"`
		}
		out := map[string]tv{}
		for k, v := range a.store.Snapshot() {
			out[k] = tv{v.V, v.Q.String(), v.TS}
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(out)
	})
	return mux
}
