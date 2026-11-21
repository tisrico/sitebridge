// Command devsim runs simulated site devices as Modbus TCP servers: a battery
// energy storage system and a site power meter.
package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tisrico/sitebridge/internal/sim"
)

func main() {
	batteryAddr := flag.String("battery", "127.0.0.1:5020", "battery Modbus TCP listen address")
	meterAddr := flag.String("meter", "127.0.0.1:5021", "meter Modbus TCP listen address")
	soc := flag.Float64("soc", 60, "initial battery state of charge (%)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	bat := sim.NewBattery(*soc)
	bs, err := sim.Serve(*batteryAddr, bat)
	if err != nil {
		log.Error("battery", "err", err)
		os.Exit(1)
	}
	defer bs.Stop()
	ms, err := sim.Serve(*meterAddr, sim.NewMeter(bat))
	if err != nil {
		log.Error("meter", "err", err)
		os.Exit(1)
	}
	defer ms.Stop()
	log.Info("simulators running", "battery", *batteryAddr, "meter", *meterAddr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	report := time.NewTicker(10 * time.Second)
	defer report.Stop()
	for {
		select {
		case <-sig:
			return
		case now := <-t.C:
			bat.Step(now)
		case <-report.C:
			log.Info("battery", "setpoint_kw", bat.Setpoint())
		}
	}
}
