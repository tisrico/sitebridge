# sitebridge

An edge bridge for an industrial energy site, written in Go. It runs on an
embedded Linux controller and:

- polls **Modbus TCP** field devices (a battery system and a site meter in the
  examples) using a configurable register map;
- exposes them through **one consistent OPC UA address space**, with device
  health mapped onto OPC UA status codes, and forwards OPC UA setpoint writes
  back to the devices;
- writes telemetry to a **durable SQLite outbox** and delivers it to a cloud
  endpoint over an unreliable network, in order, with acknowledgements;
- runs as one half of an **active/standby pair**, so only one node is ever the
  Modbus master for a device, and the standby takes over when the active node
  fails.

It ships as a single static, cgo-free binary for `linux/arm64`, `linux/armv7`
and `linux/amd64`, with a systemd unit that uses the notify watchdog.

```
                         ┌──────────── sitebridge (active) ─────────────┐
 SCADA / HMI  ◄─OPC UA──►│ opcuasrv ◄── tagstore ◄── modbusdev poller   │◄─Modbus TCP─► BESS
 (reads, setpoints)      │    │ setpoints (latest wins, retried)  ▲     │◄─Modbus TCP─► meter
                         │    └───────────────────────────────────┘     │
                         │ sampler ──► SQLite outbox ──► uploader ──────┼──HTTPS──► cloud ingest
                         │ ha elector ◄──── UDP heartbeats ────┐        │
                         └─────────────────────────────────────┼────────┘
                         ┌──────────── sitebridge (standby) ───┴────────┐
                         │ not polling; ready to take over              │
                         └──────────────────────────────────────────────┘
```

## Quick start

Requires Go 1.25+ and Python 3 (used only by the demo script).

```sh
make all     # vet, test, build into bin/
make demo    # full end-to-end demo on localhost
```

The demo starts the device simulators, a mock cloud that fails 20% of requests
and drops 10% of acknowledgements, and both nodes. It then writes a setpoint
over OPC UA, `kill -9`s the active node, restarts it, stops the devices, and
prints what the cloud received. Sample output:

```
== kill -9 the active node A
B became active after 1.33s
== restart A: it rejoins as standby (non-revertive) and drains its own backlog
A: standby   B: active
== stop the device simulators: values keep their last reading, quality goes Bad
ns=1;s=bess1.soc   60   StatusBadCommunicationError (0x80050000)
== cloud view: one stream per node, no gaps
```

To run the pieces by hand:

```sh
bin/devsim                                   # battery on :5020, meter on :5021
bin/mockcloud -fail-rate 0.3                 # ingest on :9000, GET /stats
bin/sitebridge -config examples/node-a.yaml  # OPC UA :4840, status :8081
bin/sitebridge -config examples/node-b.yaml  # OPC UA :4841, status :8082

bin/uactl -e opc.tcp://127.0.0.1:4840 read 'ns=1;s=bess1.soc'
bin/uactl -e opc.tcp://127.0.0.1:4840 write 'ns=1;s=bess1.power_setpoint.setpoint' -150
curl -s 127.0.0.1:8081/status
```

## Design

### Southbound: one Modbus master per device (`internal/modbusdev`)

- Each device has a single poller goroutine that owns the TCP connection.
  Reads and setpoint writes are both serialised through it, so requests never
  interleave on the wire and there is no locking around the client.
- Tags are grouped into as few reads as possible (`PlanReads`): tags of the
  same register kind merge when the gap between them is at most `max_gap`
  registers and the read stays within the 125-register Modbus limit. This
  saves round trips on slow links and gives a consistent snapshot of related
  registers.
- Errors are classified:
  - **Modbus exception** (illegal address/function/value): only that block's
    tags go to `ConfigError`; the link stays up and polling continues at the
    normal rate.
  - **Device busy/failure, gateway errors**: tags go to `CommFailure`; the link
    stays up and polling backs off.
  - **Transport error or timeout**: all tags go to `CommFailure` and the
    connection is dropped and re-dialled with jittered exponential backoff. A
    late reply after a timeout would desynchronise Modbus/TCP transaction IDs,
    so reconnecting is the safe choice.
- Values are decoded with per-tag type (`uint16`, `int16`, `uint32`, `int32`,
  `float32`), word order, byte order, scale and offset. Encoding for writes
  range-checks before anything is sent.
- Register layout is set per device. `word_order` (`high_first`, the default,
  or `low_first`) orders the two registers of a 32-bit value. `byte_order`
  (`little`, the default, or `big`) orders the two bytes inside each register,
  for 16-bit and 32-bit values, on both reads and writes. For a 32-bit value
  with bytes `A B C D` (`A` most significant), the bytes on the wire are shown
  below. Standard Modbus devices use big-endian registers, so set
  `byte_order: big` for them.

  | `word_order` | `byte_order` | On the wire |
  |---|---|---|
  | `high_first` | `big` | `AB CD` |
  | `low_first` | `big` | `CD AB` |
  | `high_first` | `little` | `BA DC` |
  | `low_first` | `little` | `DC BA` |

  ```yaml
  devices:
    - name: meter1
      word_order: low_first
      byte_order: big
      # address, tags and other settings as usual
  ```

### Northbound: a consistent OPC UA interface (`internal/opcuasrv`)

```
Objects/urn:sitebridge:site1/
  sitebridge.role                     "active" | "standby"
  bess1/
    bess1.soc                         Double, read-only
    bess1.power_setpoint              Double, read-only (device readback)
    bess1.power_setpoint.setpoint     Double, writable
  meter1/ ...
```

- **Quality is part of the value.** A client sees the last known value together
  with `Good`, `BadCommunicationError`, `BadConfigurationError`,
  `BadOutOfService` (standby) or `BadWaitingForInitialData`, plus the source
  timestamp of the reading. A stale value never looks like a live one.
- **Setpoints are desired state, not commands.** A write records the latest
  desired value per tag. A worker applies it and retries on failure until it
  succeeds or a newer value replaces it, so a burst of writes during a comms
  dropout ends with the device at the latest value, not replaying the history.
- **No stale commands after failover.** Writes received while standby are
  ignored, and desired state is cleared on every role change. A node that
  becomes active never applies a setpoint that was accepted before.
- Subscriptions get change notifications only when a value or its quality
  actually changes.

### Telemetry: store-and-forward (`internal/telemetry`)

- A sampler snapshots all tags every `sample_interval` into the outbox. Sampling
  is separate from polling, so upload volume is set by telemetry policy, not by
  how fast a device is polled.
- **Durable before eligible.** Each record is committed to SQLite (WAL,
  `synchronous=FULL` by default) before the uploader can see it. It is deleted
  only when the cloud acknowledges it. Delivery is at-least-once across process
  crashes, reboots and outages.
- **Sequence numbers are never reused.** They come from an `AUTOINCREMENT` key,
  so they keep increasing even after the outbox drains or the process restarts.
  A random stream ID is created with the database; if the database is lost the
  receiver sees a new stream, not a sequence that silently restarted. The
  receiver de-duplicates on `(source, stream, seq)`.
- **Draining after an outage:** batches (gzip JSON) are sent back to back while
  the backlog exceeds one batch, with jittered exponential backoff on failure.
  The receiver may acknowledge part of a batch. A `413` halves the batch size,
  which then grows back after successes.
- **Bounded storage:** when the backlog reaches `max_backlog` the oldest records
  are discarded, because the newest data matters most and the device must not
  fill its flash. Discards are counted on `/status` and show up at the receiver
  as a gap.
- `synchronous: NORMAL` is available when write endurance matters more. WAL
  then still protects against process crashes but may lose the last
  transactions on power loss.

### Redundancy: active/standby (`internal/ha`)

- Each node sends a UDP heartbeat every `heartbeat_interval` with its ID,
  priority, role, term and sequence number.
- Only the **active** node polls devices, samples telemetry and accepts
  setpoints. Both nodes keep their OPC UA server and uploader running, so a
  standby still drains any backlog it built up while it was active.
- **Takeover:** a standby becomes active when it has not heard the active node
  for `failover_timeout` (default 1 s, i.e. 5 missed heartbeats). On a clean
  shutdown the active node sends a final `leaving` heartbeat and the standby
  takes over immediately (about 5 ms in tests rather than 1 s).
- **Start-up:** a node that has never heard its peer waits one full timeout
  before claiming the site. When both start together, priority and then node
  ID decide.
- **Non-revertive:** a recovered node rejoins as standby even if it has the
  higher priority, so one fault causes one switchover, not two.
- **Split brain:** each takeover increments a term. If both nodes are active
  (the inter-node link failed and then healed), the lower term steps down at
  once.
- The decision logic is a pure function (`decide`) and is covered by table
  tests. The UDP transport is covered by pair tests, including a simulated
  crash.

### Process supervision (`deploy/sitebridge.service`, `internal/sdnotify`)

`Type=notify` with `WatchdogSec`: the binary reports `READY=1` once it is
serving and sends `WATCHDOG=1` from its main loop. It implements the protocol
directly, without libsystemd or cgo. If the main loop stalls, systemd restarts
the process, and meanwhile the peer takes over because heartbeats stop.

## Failure modes

| Failure | Behaviour |
|---|---|
| Device unplugged / powered off | Tags keep last value, quality `BadCommunicationError`; reconnect with backoff; recovers on its own |
| Wrong register in config | Only that tag goes `BadConfigurationError`; the rest keep polling |
| Cloud unreachable for hours | Records accumulate durably; drain in order when the link returns |
| Cloud stores a batch but the reply is lost | Batch is resent; receiver de-duplicates on `(stream, seq)` |
| `kill -9` / power loss during a write | SQLite WAL rollback; tested by SIGKILLing a writer mid-loop and checking integrity and gap-free sequences |
| Active node crashes | Standby takes over after `failover_timeout` (1.3 s in the demo) |
| Active node shut down cleanly | Standby takes over at once |
| Setpoint write fails (device busy) | Retried every second until it succeeds or is superseded |
| Inter-node link fails, both reach devices | **Both become active** (see limitations); resolved by term when the link heals |

## Testing

```sh
make race   # all tests with the race detector
```

- **Codec and read planning:** table tests for every type, word order,
  byte order, scaling, range limits and block merging.
- **Poller:** runs against the simulator over real TCP. Covers reads, a bad
  register isolated to its own tag, setpoint writes, the simulator being
  stopped and restarted, and writes while disconnected.
- **Outbox:** FIFO and ack, reopen persistence, sequence numbers never reused,
  backlog cap, and repeated `kill -9` of a child process mid-write followed by
  an integrity check.
- **Uploader:** against the mock ingest with 50% request failures and 30% lost
  acks. Every record must arrive exactly once after de-duplication, in order.
  Also covers an outage followed by recovery, and `413` batch shrinking.
- **HA:** table tests for every transition, plus UDP pair tests for clean
  handover, crash failover timing and non-revertive rejoin.
- **OPC UA:** a real gopcua client reads values and status codes, is refused
  writes to read-only tags, and checks setpoint behaviour on standby, after
  failover, and across write failures.

## Known limitations and next steps

- **Fencing.** Two nodes cannot form a quorum. A production pair needs a third
  opinion before taking over, for example a witness on the device network or a
  hardware interlock that physically enables one controller's field port.
- **OPC UA security.** The examples use security policy `None` and anonymous
  access for ease of testing. The library supports `Basic256Sha256`
  sign-and-encrypt with certificates, which should be required on site.
- **Uplink security.** Telemetry is sent over plain HTTP(S) with no client
  authentication. A real device would use mTLS with a key held in a secure
  element.
- **More protocols.** Modbus RTU over RS-485 (the library supports it), and an
  OPC UA *client* side for bridging to PLC runtimes such as CODESYS that expose
  their own OPC UA server.
- **Config hot reload,** and Prometheus metrics alongside `/status`.

## Layout

```
cmd/sitebridge     the bridge
cmd/devsim         Modbus TCP battery and meter simulators
cmd/mockcloud      ingest service with fault injection
cmd/uactl          minimal OPC UA read/write client
internal/config    YAML config, defaults, validation
internal/modbusdev poller, codec, read planning
internal/opcuasrv  OPC UA address space, quality mapping, setpoints
internal/telemetry SQLite outbox, uploader, sampler
internal/ingest    receiver logic (used by mockcloud and tests)
internal/ha        active/standby elector
internal/tagstore  latest value and quality per tag
internal/sdnotify  systemd notify and watchdog
internal/sim       device models
examples/          configs for a two-node pair
deploy/            systemd unit
```
