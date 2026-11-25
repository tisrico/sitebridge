// Package modbusdev polls Modbus TCP devices and converts registers to
// engineering values.
package modbusdev

import (
	"fmt"
	"math"
	"math/bits"

	"github.com/tisrico/sitebridge/internal/config"
)

// Decode converts raw registers (already sliced to the tag's width) into a
// scaled engineering value: value = raw*scale + offset.
func Decode(regs []uint16, t config.Tag, wordOrder string, byteOrder string) (float64, error) {
	w, err := config.Width(t.Type)
	if err != nil {
		return 0, err
	}
	if len(regs) < int(w) {
		return 0, fmt.Errorf("tag %s: need %d registers, got %d", t.Name, w, len(regs))
	}
	regs = applyByteOrder(regs[:w], byteOrder)
	var raw float64
	switch t.Type {
	case "uint16":
		raw = float64(regs[0])
	case "int16":
		raw = float64(int16(regs[0]))
	case "uint32", "int32", "float32":
		u := join32(regs[0], regs[1], wordOrder)
		switch t.Type {
		case "uint32":
			raw = float64(u)
		case "int32":
			raw = float64(int32(u))
		default:
			f := math.Float32frombits(u)
			if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
				return 0, fmt.Errorf("tag %s: non-finite float", t.Name)
			}
			raw = float64(f)
		}
	}
	return raw*t.Scale + t.Offset, nil
}

// Encode converts an engineering value into raw registers for writing,
// rejecting values that do not fit the register type.
func Encode(v float64, t config.Tag, wordOrder string, byteOrder string) ([]uint16, error) {
	regs, err := encodeWords(v, t, wordOrder)
	if err != nil {
		return nil, err
	}
	return applyByteOrder(regs, byteOrder), nil
}

// encodeWords converts a value into registers arranged in wordOrder, before
// any byte swapping.
func encodeWords(v float64, t config.Tag, wordOrder string) ([]uint16, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil, fmt.Errorf("tag %s: value must be finite", t.Name)
	}
	raw := (v - t.Offset) / t.Scale
	if t.Type == "float32" {
		if math.Abs(raw) > math.MaxFloat32 {
			return nil, fmt.Errorf("tag %s: %v out of range", t.Name, v)
		}
		hi, lo := split32(math.Float32bits(float32(raw)), wordOrder)
		return []uint16{hi, lo}, nil
	}
	r := math.Round(raw)
	lo, hi, err := intRange(t.Type)
	if err != nil {
		return nil, err
	}
	if r < lo || r > hi {
		return nil, fmt.Errorf("tag %s: %v out of range for %s", t.Name, v, t.Type)
	}
	switch t.Type {
	case "uint16":
		return []uint16{uint16(r)}, nil
	case "int16":
		return []uint16{uint16(int16(r))}, nil
	case "uint32":
		a, b := split32(uint32(r), wordOrder)
		return []uint16{a, b}, nil
	default: // int32
		a, b := split32(uint32(int32(r)), wordOrder)
		return []uint16{a, b}, nil
	}
}

// applyByteOrder swaps the two bytes of every register for little-endian
// devices. The swap is its own inverse, so reads and writes both use it. It
// never modifies regs, because tags may share registers within a read block.
func applyByteOrder(regs []uint16, byteOrder string) []uint16 {
	if byteOrder != config.LittleEndian {
		return regs
	}
	out := make([]uint16, len(regs))
	for i, r := range regs {
		out[i] = bits.ReverseBytes16(r)
	}
	return out
}

func intRange(typ string) (float64, float64, error) {
	switch typ {
	case "uint16":
		return 0, math.MaxUint16, nil
	case "int16":
		return math.MinInt16, math.MaxInt16, nil
	case "uint32":
		return 0, math.MaxUint32, nil
	case "int32":
		return math.MinInt32, math.MaxInt32, nil
	}
	return 0, 0, fmt.Errorf("unknown type %q", typ)
}

func join32(a, b uint16, wordOrder string) uint32 {
	if wordOrder == config.LowWordFirst {
		a, b = b, a
	}
	return uint32(a)<<16 | uint32(b)
}

// split32 returns the two registers in wire order.
func split32(u uint32, wordOrder string) (uint16, uint16) {
	hi, lo := uint16(u>>16), uint16(u)
	if wordOrder == config.LowWordFirst {
		return lo, hi
	}
	return hi, lo
}
