#!/usr/bin/env bash
# End-to-end demo on one machine: simulated devices, a flaky mock cloud, and an
# active/standby pair. Shows setpoint writes over OPC UA, crash failover,
# non-revertive rejoin, device loss, and gap-free telemetry delivery.
set -euo pipefail
cd "$(dirname "$0")/.."

LOG=${LOG:-/tmp/sitebridge-demo}
rm -rf "$LOG" data && mkdir -p "$LOG" data
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT

role() { curl -s "127.0.0.1:$1/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["role"])'; }
step() { printf '\n== %s\n' "$*"; }
ua() { bin/uactl -e "opc.tcp://127.0.0.1:$1" "${@:2}"; }

step "start simulators and a mock cloud that fails 20% of requests and loses 10% of acks"
bin/devsim >"$LOG/devsim.log" 2>&1 & pids+=($!)
bin/mockcloud -fail-rate 0.2 -lose-acks 0.1 >"$LOG/cloud.log" 2>&1 & pids+=($!)
sleep 0.5

step "start node A (priority 100) and node B (priority 50)"
bin/sitebridge -config examples/node-a.yaml >"$LOG/a.log" 2>&1 & A=$!; pids+=($A)
bin/sitebridge -config examples/node-b.yaml >"$LOG/b.log" 2>&1 & B=$!; pids+=($B)
sleep 4
echo "A: $(role 8081)   B: $(role 8082)"

step "read live values through OPC UA on A"
ua 4840 read 'ns=1;s=sitebridge.role' 'ns=1;s=bess1.soc' 'ns=1;s=bess1.active_power' 'ns=1;s=meter1.site_power'

step "write a -150 kW (charge) setpoint through OPC UA; the battery ramps to it"
ua 4840 write 'ns=1;s=bess1.power_setpoint.setpoint' -150
sleep 3
ua 4840 read 'ns=1;s=bess1.power_setpoint' 'ns=1;s=bess1.active_power'

step "kill -9 the active node A"
kill -9 "$A"
start=$(python3 -c "import time; print(time.time())")
until [ "$(role 8082)" = active ]; do sleep 0.05; done
python3 -c "import time; print('B became active after %.2fs' % (time.time() - $start))"
sleep 1
ua 4841 read 'ns=1;s=sitebridge.role' 'ns=1;s=bess1.soc'

step "restart A: it rejoins as standby (non-revertive) and drains its own backlog"
bin/sitebridge -config examples/node-a.yaml >>"$LOG/a.log" 2>&1 & A=$!; pids+=($A)
sleep 4
echo "A: $(role 8081)   B: $(role 8082)"

step "stop the device simulators: values keep their last reading, quality goes Bad"
kill "${pids[0]}"
sleep 2.5
ua 4841 read 'ns=1;s=bess1.soc'

step "cloud view: one stream per node, no gaps (duplicates from lost acks are de-duplicated)"
curl -s 127.0.0.1:9000/stats

echo
echo "logs in $LOG"
