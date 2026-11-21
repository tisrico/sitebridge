// Package sim provides Modbus TCP device simulators (a battery and a power
// meter) so the bridge can be developed and tested without hardware.
package sim

import (
	"math"
	"sync"
	"time"

	"github.com/simonvetter/modbus"
)

// Battery register map (also used by examples/*.yaml).
//
//	input 0     soc            uint16  0.1 %
//	input 1     active_power   int16   1 kW   (+ discharge, - charge)
//	input 2     dc_voltage     uint16  0.1 V
//	input 3     cell_temp      int16   0.1 degC
//	input 4-5   energy_out     uint32  1 kWh
//	holding 100 power_setpoint int16   1 kW   (writable)
//	holding 101 heartbeat      uint16  increments every update
const (
	BatterySoC      = 0
	BatteryPower    = 1
	BatteryVoltage  = 2
	BatteryTemp     = 3
	BatteryEnergy   = 4
	BatterySetpoint = 100
	BatteryCounter  = 101
)

// Battery is a simple battery energy storage model.
type Battery struct {
	mu          sync.Mutex
	CapacityKWh float64
	MaxKW       float64
	RampKWps    float64

	soc      float64 // %
	powerKW  float64
	setpoint float64
	tempC    float64
	energy   float64 // kWh discharged
	counter  uint16
	last     time.Time
}

// NewBattery returns a battery at the given state of charge.
func NewBattery(socPct float64) *Battery {
	return &Battery{CapacityKWh: 1000, MaxKW: 500, RampKWps: 100, soc: socPct, tempC: 25, last: time.Now()}
}

// Step advances the model to now.
func (b *Battery) Step(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	dt := now.Sub(b.last).Seconds()
	b.last = now
	if dt <= 0 {
		return
	}
	target := math.Max(-b.MaxKW, math.Min(b.MaxKW, b.setpoint))
	if b.soc <= 5 && target > 0 || b.soc >= 95 && target < 0 {
		target = 0 // protect the battery at SoC limits
	}
	step := b.RampKWps * dt
	switch {
	case b.powerKW < target:
		b.powerKW = math.Min(target, b.powerKW+step)
	case b.powerKW > target:
		b.powerKW = math.Max(target, b.powerKW-step)
	}
	kwh := b.powerKW * dt / 3600
	b.soc = math.Max(0, math.Min(100, b.soc-kwh/b.CapacityKWh*100))
	if kwh > 0 {
		b.energy += kwh
	}
	b.tempC += (25 + math.Abs(b.powerKW)/b.MaxKW*15 - b.tempC) * math.Min(1, dt/60)
	b.counter++
}

// Setpoint returns the current power setpoint in kW.
func (b *Battery) Setpoint() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.setpoint
}

func (b *Battery) inputRegs() []uint16 {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := uint32(b.energy)
	return []uint16{
		uint16(math.Round(b.soc * 10)),
		uint16(int16(math.Round(b.powerKW))),
		uint16(math.Round((700 + b.soc*1.5) * 10)),
		uint16(int16(math.Round(b.tempC * 10))),
		uint16(e >> 16), uint16(e),
	}
}

// HandleInputRegisters implements modbus.RequestHandler.
func (b *Battery) HandleInputRegisters(req *modbus.InputRegistersRequest) ([]uint16, error) {
	return window(b.inputRegs(), 0, req.Addr, req.Quantity)
}

// HandleHoldingRegisters implements modbus.RequestHandler.
func (b *Battery) HandleHoldingRegisters(req *modbus.HoldingRegistersRequest) ([]uint16, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if req.IsWrite {
		if req.Addr != BatterySetpoint || req.Quantity != 1 {
			return nil, modbus.ErrIllegalDataAddress
		}
		sp := float64(int16(req.Args[0]))
		if math.Abs(sp) > b.MaxKW {
			return nil, modbus.ErrIllegalDataValue
		}
		b.setpoint = sp
		return nil, nil
	}
	regs := []uint16{uint16(int16(math.Round(b.setpoint))), b.counter}
	return window(regs, BatterySetpoint, req.Addr, req.Quantity)
}

// HandleCoils implements modbus.RequestHandler.
func (b *Battery) HandleCoils(*modbus.CoilsRequest) ([]bool, error) {
	return nil, modbus.ErrIllegalFunction
}

// HandleDiscreteInputs implements modbus.RequestHandler.
func (b *Battery) HandleDiscreteInputs(*modbus.DiscreteInputsRequest) ([]bool, error) {
	return nil, modbus.ErrIllegalFunction
}

// Meter register map (input registers, float32, high word first):
//
//	0-1 site_power  kW
//	2-3 frequency   Hz
//	4-5 voltage     V
type Meter struct {
	mu      sync.Mutex
	battery *Battery
	start   time.Time
}

// NewMeter returns a meter at a site whose load varies over time; the
// battery output offsets it.
func NewMeter(b *Battery) *Meter { return &Meter{battery: b, start: time.Now()} }

// HandleInputRegisters implements modbus.RequestHandler.
func (m *Meter) HandleInputRegisters(req *modbus.InputRegistersRequest) ([]uint16, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := time.Since(m.start).Seconds()
	load := 800 + 200*math.Sin(t/30)
	bp := 0.0
	if m.battery != nil {
		m.battery.mu.Lock()
		bp = m.battery.powerKW
		m.battery.mu.Unlock()
	}
	vals := []float32{float32(load - bp), float32(50 + 0.02*math.Sin(t/7)), float32(415 + 2*math.Sin(t/11))}
	regs := make([]uint16, 0, 6)
	for _, v := range vals {
		u := math.Float32bits(v)
		regs = append(regs, uint16(u>>16), uint16(u))
	}
	return window(regs, 0, req.Addr, req.Quantity)
}

// HandleHoldingRegisters implements modbus.RequestHandler.
func (m *Meter) HandleHoldingRegisters(*modbus.HoldingRegistersRequest) ([]uint16, error) {
	return nil, modbus.ErrIllegalDataAddress
}

// HandleCoils implements modbus.RequestHandler.
func (m *Meter) HandleCoils(*modbus.CoilsRequest) ([]bool, error) {
	return nil, modbus.ErrIllegalFunction
}

// HandleDiscreteInputs implements modbus.RequestHandler.
func (m *Meter) HandleDiscreteInputs(*modbus.DiscreteInputsRequest) ([]bool, error) {
	return nil, modbus.ErrIllegalFunction
}

func window(regs []uint16, base, addr, qty uint16) ([]uint16, error) {
	if addr < base || int(addr-base)+int(qty) > len(regs) {
		return nil, modbus.ErrIllegalDataAddress
	}
	out := make([]uint16, qty)
	copy(out, regs[addr-base:])
	return out, nil
}

// Serve starts a Modbus TCP server for handler at addr (host:port).
func Serve(addr string, handler modbus.RequestHandler) (*modbus.ModbusServer, error) {
	s, err := modbus.NewServer(&modbus.ServerConfiguration{
		URL:        "tcp://" + addr,
		Timeout:    30 * time.Second,
		MaxClients: 8,
	}, handler)
	if err != nil {
		return nil, err
	}
	if err := s.Start(); err != nil {
		return nil, err
	}
	return s, nil
}
