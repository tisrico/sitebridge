package modbusdev

import (
	"math"
	"testing"

	"github.com/tisrico/sitebridge/internal/config"
)

func tag(typ string, scale, offset float64) config.Tag {
	return config.Tag{Name: "t", Type: typ, Scale: scale, Offset: offset, Kind: config.KindHolding}
}

func TestDecode(t *testing.T) {
	cases := []struct {
		name  string
		regs  []uint16
		tag   config.Tag
		order string
		want  float64
	}{
		{"uint16 scaled", []uint16{875}, tag("uint16", 0.1, 0), config.HighWordFirst, 87.5},
		{"int16 negative", []uint16{0xFF9C}, tag("int16", 1, 0), config.HighWordFirst, -100},
		{"offset", []uint16{100}, tag("int16", 0.1, -40), config.HighWordFirst, -30},
		{"uint32 high first", []uint16{0x0001, 0x0002}, tag("uint32", 1, 0), config.HighWordFirst, 65538},
		{"uint32 low first", []uint16{0x0002, 0x0001}, tag("uint32", 1, 0), config.LowWordFirst, 65538},
		{"int32 negative", []uint16{0xFFFF, 0xFFFE}, tag("int32", 1, 0), config.HighWordFirst, -2},
		{"float32", []uint16{0x4248, 0x0000}, tag("float32", 1, 0), config.HighWordFirst, 50},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Decode(c.regs, c.tag, c.order)
			if err != nil {
				t.Fatal(err)
			}
			if math.Abs(got-c.want) > 1e-9 {
				t.Errorf("got %v want %v", got, c.want)
			}
		})
	}
}

func TestDecodeRejectsNaN(t *testing.T) {
	if _, err := Decode([]uint16{0x7FC0, 0}, tag("float32", 1, 0), config.HighWordFirst); err == nil {
		t.Fatal("expected error for NaN")
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	for _, typ := range []string{"uint16", "int16", "uint32", "int32", "float32"} {
		for _, order := range []string{config.HighWordFirst, config.LowWordFirst} {
			tg := tag(typ, 0.1, 0)
			v := 12.3
			regs, err := Encode(v, tg, order)
			if err != nil {
				t.Fatalf("%s/%s: %v", typ, order, err)
			}
			got, err := Decode(regs, tg, order)
			if err != nil {
				t.Fatal(err)
			}
			if math.Abs(got-v) > 1e-4 {
				t.Errorf("%s/%s: round trip got %v want %v", typ, order, got, v)
			}
		}
	}
}

func TestEncodeRange(t *testing.T) {
	if _, err := Encode(-1, tag("uint16", 1, 0), config.HighWordFirst); err == nil {
		t.Error("expected error for negative uint16")
	}
	if _, err := Encode(40000, tag("int16", 1, 0), config.HighWordFirst); err == nil {
		t.Error("expected error for int16 overflow")
	}
	if _, err := Encode(math.NaN(), tag("int16", 1, 0), config.HighWordFirst); err == nil {
		t.Error("expected error for NaN")
	}
	regs, err := Encode(-250, tag("int16", 1, 0), config.HighWordFirst)
	if err != nil || regs[0] != uint16(0xFF06) {
		t.Errorf("int16 -250: got %v %v", regs, err)
	}
}

func TestPlanReads(t *testing.T) {
	tags := []config.Tag{
		{Name: "a", Kind: config.KindInput, Address: 0, Type: "uint16"},
		{Name: "b", Kind: config.KindInput, Address: 1, Type: "int16"},
		{Name: "c", Kind: config.KindInput, Address: 4, Type: "uint32"}, // gap of 2 after b
		{Name: "d", Kind: config.KindInput, Address: 50, Type: "uint16"},
		{Name: "e", Kind: config.KindHolding, Address: 100, Type: "int16"},
	}
	blocks := PlanReads(tags, 2)
	if len(blocks) != 3 {
		t.Fatalf("want 3 blocks, got %d: %+v", len(blocks), blocks)
	}
	// holding first
	if b := blocks[0]; b.Kind != config.KindHolding || b.Start != 100 || b.Count != 1 {
		t.Errorf("holding block: %+v", b)
	}
	if b := blocks[1]; b.Start != 0 || b.Count != 6 || len(b.Tags) != 3 {
		t.Errorf("merged input block: %+v", b)
	}
	if b := blocks[2]; b.Start != 50 || b.Count != 1 {
		t.Errorf("separate input block: %+v", b)
	}

	// with no gap allowed, c is split out
	if n := len(PlanReads(tags, 0)); n != 4 {
		t.Errorf("maxGap 0: want 4 blocks, got %d", n)
	}
}

func TestPlanReadsRespectsLimit(t *testing.T) {
	tags := []config.Tag{
		{Name: "a", Kind: config.KindHolding, Address: 0, Type: "uint16"},
		{Name: "b", Kind: config.KindHolding, Address: 124, Type: "uint32"}, // would make 126
	}
	if n := len(PlanReads(tags, 200)); n != 2 {
		t.Errorf("want 2 blocks, got %d", n)
	}
}
