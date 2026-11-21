package config

import (
	"strings"
	"testing"
	"time"
)

const minimal = `
node: {id: a}
telemetry: {db: /tmp/x.db}
devices:
  - name: bess1
    address: tcp://127.0.0.1:5020
    tags:
      - {name: soc, kind: input, address: 0, type: uint16, scale: 0.1}
`

func TestDefaults(t *testing.T) {
	c, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if c.HA.HeartbeatInterval != 200*time.Millisecond || c.HA.FailoverTimeout != time.Second {
		t.Errorf("ha defaults: %+v", c.HA)
	}
	if c.Telemetry.Source != "a" || c.Telemetry.Synchronous != "FULL" {
		t.Errorf("telemetry defaults: %+v", c.Telemetry)
	}
	d := c.Devices[0]
	if d.UnitID != 1 || d.PollInterval != time.Second || d.WordOrder != HighWordFirst {
		t.Errorf("device defaults: %+v", d)
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	_, err := Parse([]byte(minimal + "\nbogus: 1\n"))
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestValidationErrors(t *testing.T) {
	cases := map[string]string{
		"writable input": `
node: {id: a}
telemetry: {db: x}
devices:
  - name: d
    address: tcp://h:1
    tags: [{name: p, kind: input, address: 0, type: int16, writable: true}]`,
		"bad type": `
node: {id: a}
telemetry: {db: x}
devices:
  - name: d
    address: tcp://h:1
    tags: [{name: p, address: 0, type: int64}]`,
		"duplicate tag": `
node: {id: a}
telemetry: {db: x}
devices:
  - name: d
    address: tcp://h:1
    tags: [{name: p, address: 0, type: int16}, {name: p, address: 1, type: int16}]`,
		"address overflow": `
node: {id: a}
telemetry: {db: x}
devices:
  - name: d
    address: tcp://h:1
    tags: [{name: p, address: 65535, type: uint32}]`,
		"ha timeout too short": `
node: {id: a}
ha: {enabled: true, listen: ":1", peer: "h:2", heartbeat_interval: 500ms, failover_timeout: 1s}
telemetry: {db: x}
devices:
  - name: d
    address: tcp://h:1
    tags: [{name: p, address: 0, type: int16}]`,
		"missing node id": strings.Replace(minimal, "node: {id: a}", "", 1),
	}
	for name, y := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(y)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestExampleConfigsAreValid(t *testing.T) {
	for _, f := range []string{"../../examples/node-a.yaml", "../../examples/node-b.yaml"} {
		if _, err := Load(f); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}
