// Package config loads and validates the sitebridge YAML configuration.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the full site configuration for one sitebridge node.
type Config struct {
	Node      Node      `yaml:"node"`
	HA        HA        `yaml:"ha"`
	OPCUA     OPCUA     `yaml:"opcua"`
	Telemetry Telemetry `yaml:"telemetry"`
	HTTP      HTTP      `yaml:"http"`
	Devices   []Device  `yaml:"devices"`
}

// Node identifies this instance within an active/standby pair.
type Node struct {
	ID       string `yaml:"id"`
	Priority int    `yaml:"priority"`
}

// HA configures active/standby redundancy between two nodes.
type HA struct {
	Enabled           bool          `yaml:"enabled"`
	Listen            string        `yaml:"listen"`
	Peer              string        `yaml:"peer"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	FailoverTimeout   time.Duration `yaml:"failover_timeout"`
}

// OPCUA configures the northbound OPC UA server.
type OPCUA struct {
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	Namespace string `yaml:"namespace"`
}

// Telemetry configures the store-and-forward uplink.
type Telemetry struct {
	DB              string        `yaml:"db"`
	URL             string        `yaml:"url"`
	Source          string        `yaml:"source"`
	SampleInterval  time.Duration `yaml:"sample_interval"`
	FlushInterval   time.Duration `yaml:"flush_interval"`
	BatchMaxRecords int           `yaml:"batch_max_records"`
	MaxBacklog      int           `yaml:"max_backlog"`
	Synchronous     string        `yaml:"synchronous"`
	RequestTimeout  time.Duration `yaml:"request_timeout"`
	BackoffMin      time.Duration `yaml:"backoff_min"`
	BackoffMax      time.Duration `yaml:"backoff_max"`
}

// HTTP configures the local status endpoint.
type HTTP struct {
	Listen string `yaml:"listen"`
}

// Device is one southbound Modbus TCP device.
type Device struct {
	Name         string        `yaml:"name"`
	Address      string        `yaml:"address"`
	UnitID       uint8         `yaml:"unit_id"`
	PollInterval time.Duration `yaml:"poll_interval"`
	Timeout      time.Duration `yaml:"timeout"`
	// WordOrder for 32-bit values: "high_first" (default) or "low_first".
	WordOrder string `yaml:"word_order"`
	// MaxGap is the largest run of unused registers that may be read
	// in order to merge two tags into one request.
	MaxGap uint16 `yaml:"max_gap"`
	Tags   []Tag  `yaml:"tags"`
}

// Tag maps one Modbus register (or register pair) to an engineering value.
type Tag struct {
	Name     string  `yaml:"name"`
	Kind     string  `yaml:"kind"` // "holding" or "input"
	Address  uint16  `yaml:"address"`
	Type     string  `yaml:"type"` // uint16, int16, uint32, int32, float32
	Scale    float64 `yaml:"scale"`
	Offset   float64 `yaml:"offset"`
	Unit     string  `yaml:"unit"`
	Writable bool    `yaml:"writable"`
}

// Register kinds.
const (
	KindHolding = "holding"
	KindInput   = "input"
)

// Word orders.
const (
	HighWordFirst = "high_first"
	LowWordFirst  = "low_first"
)

// Width returns the number of 16-bit registers a tag type occupies.
func Width(typ string) (uint16, error) {
	switch typ {
	case "uint16", "int16":
		return 1, nil
	case "uint32", "int32", "float32":
		return 2, nil
	}
	return 0, fmt.Errorf("unknown tag type %q", typ)
}

// Key returns the store key for a device tag.
func Key(device, tag string) string { return device + "." + tag }

// Load reads, defaults and validates a configuration file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// Parse decodes, defaults and validates configuration bytes.
func Parse(b []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.HA.HeartbeatInterval == 0 {
		c.HA.HeartbeatInterval = 200 * time.Millisecond
	}
	if c.HA.FailoverTimeout == 0 {
		c.HA.FailoverTimeout = 1 * time.Second
	}
	if c.OPCUA.Host == "" {
		c.OPCUA.Host = "0.0.0.0"
	}
	if c.OPCUA.Port == 0 {
		c.OPCUA.Port = 4840
	}
	if c.OPCUA.Namespace == "" {
		c.OPCUA.Namespace = "urn:sitebridge"
	}
	t := &c.Telemetry
	if t.Source == "" {
		t.Source = c.Node.ID
	}
	if t.SampleInterval == 0 {
		t.SampleInterval = 5 * time.Second
	}
	if t.FlushInterval == 0 {
		t.FlushInterval = 2 * time.Second
	}
	if t.BatchMaxRecords == 0 {
		t.BatchMaxRecords = 500
	}
	if t.MaxBacklog == 0 {
		t.MaxBacklog = 100000
	}
	if t.Synchronous == "" {
		t.Synchronous = "FULL"
	}
	t.Synchronous = strings.ToUpper(t.Synchronous)
	if t.RequestTimeout == 0 {
		t.RequestTimeout = 10 * time.Second
	}
	if t.BackoffMin == 0 {
		t.BackoffMin = 1 * time.Second
	}
	if t.BackoffMax == 0 {
		t.BackoffMax = 5 * time.Minute
	}
	if c.HTTP.Listen == "" {
		c.HTTP.Listen = "127.0.0.1:8080"
	}
	for i := range c.Devices {
		d := &c.Devices[i]
		if d.UnitID == 0 {
			d.UnitID = 1
		}
		if d.PollInterval == 0 {
			d.PollInterval = time.Second
		}
		if d.Timeout == 0 {
			d.Timeout = time.Second
		}
		if d.WordOrder == "" {
			d.WordOrder = HighWordFirst
		}
		for j := range d.Tags {
			tg := &d.Tags[j]
			if tg.Scale == 0 {
				tg.Scale = 1
			}
			if tg.Kind == "" {
				tg.Kind = KindHolding
			}
		}
	}
}

// Validate checks the configuration for errors that would otherwise only
// show up at runtime on site.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if c.Node.ID == "" {
		add("node.id is required")
	}
	if c.HA.Enabled {
		if c.HA.Listen == "" || c.HA.Peer == "" {
			add("ha.listen and ha.peer are required when ha.enabled is true")
		}
		if c.HA.FailoverTimeout < 3*c.HA.HeartbeatInterval {
			add("ha.failover_timeout (%s) must be at least 3x ha.heartbeat_interval (%s)",
				c.HA.FailoverTimeout, c.HA.HeartbeatInterval)
		}
	}
	if c.Telemetry.DB == "" {
		add("telemetry.db is required")
	}
	if c.Telemetry.URL != "" {
		if u, err := url.Parse(c.Telemetry.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
			add("telemetry.url must be an http(s) URL")
		}
	}
	if c.Telemetry.Synchronous != "FULL" && c.Telemetry.Synchronous != "NORMAL" {
		add("telemetry.synchronous must be FULL or NORMAL")
	}
	if c.Telemetry.BackoffMin > c.Telemetry.BackoffMax {
		add("telemetry.backoff_min must not exceed telemetry.backoff_max")
	}
	if len(c.Devices) == 0 {
		add("at least one device is required")
	}

	devNames := map[string]bool{}
	for _, d := range c.Devices {
		if d.Name == "" || strings.Contains(d.Name, ".") {
			add("device name %q must be non-empty and contain no '.'", d.Name)
		}
		if devNames[d.Name] {
			add("duplicate device name %q", d.Name)
		}
		devNames[d.Name] = true
		if !strings.HasPrefix(d.Address, "tcp://") {
			add("device %s: address must be tcp://host:port", d.Name)
		}
		if d.WordOrder != HighWordFirst && d.WordOrder != LowWordFirst {
			add("device %s: word_order must be %s or %s", d.Name, HighWordFirst, LowWordFirst)
		}
		if len(d.Tags) == 0 {
			add("device %s: at least one tag is required", d.Name)
		}
		tagNames := map[string]bool{}
		for _, t := range d.Tags {
			if t.Name == "" || strings.Contains(t.Name, ".") {
				add("device %s: tag name %q must be non-empty and contain no '.'", d.Name, t.Name)
			}
			if tagNames[t.Name] {
				add("device %s: duplicate tag %q", d.Name, t.Name)
			}
			tagNames[t.Name] = true
			w, err := Width(t.Type)
			if err != nil {
				add("device %s tag %s: %v", d.Name, t.Name, err)
				continue
			}
			if int(t.Address)+int(w) > 65536 {
				add("device %s tag %s: address out of range", d.Name, t.Name)
			}
			if t.Kind != KindHolding && t.Kind != KindInput {
				add("device %s tag %s: kind must be holding or input", d.Name, t.Name)
			}
			if t.Writable && t.Kind != KindHolding {
				add("device %s tag %s: only holding registers can be writable", d.Name, t.Name)
			}
		}
	}
	return errors.Join(errs...)
}
