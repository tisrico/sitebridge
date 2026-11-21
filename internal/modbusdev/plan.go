package modbusdev

import (
	"sort"

	"github.com/tisrico/sitebridge/internal/config"
)

// MaxRegistersPerRead is the Modbus limit for a single read request.
const MaxRegistersPerRead = 125

// Block is one contiguous register read covering one or more tags.
type Block struct {
	Kind  string
	Start uint16
	Count uint16
	Tags  []int // indexes into the device's tag slice
}

// PlanReads groups tags into as few read requests as possible. Tags of the
// same register kind are merged when the gap between them is at most maxGap
// registers and the merged read stays within the Modbus limit. Fewer, larger
// reads cut round trips on slow field links and give a consistent snapshot of
// related registers.
func PlanReads(tags []config.Tag, maxGap uint16) []Block {
	type item struct {
		idx   int
		start uint16
		end   int // exclusive
	}
	byKind := map[string][]item{}
	for i, t := range tags {
		w, err := config.Width(t.Type)
		if err != nil {
			continue // rejected by config validation
		}
		byKind[t.Kind] = append(byKind[t.Kind], item{i, t.Address, int(t.Address) + int(w)})
	}

	var blocks []Block
	for _, kind := range []string{config.KindHolding, config.KindInput} {
		items := byKind[kind]
		sort.Slice(items, func(a, b int) bool { return items[a].start < items[b].start })
		var cur *Block
		curEnd := 0
		for _, it := range items {
			if cur != nil {
				newEnd := max(curEnd, it.end)
				if int(it.start) <= curEnd+int(maxGap) && newEnd-int(cur.Start) <= MaxRegistersPerRead {
					curEnd = newEnd
					cur.Count = uint16(curEnd - int(cur.Start))
					cur.Tags = append(cur.Tags, it.idx)
					continue
				}
				blocks = append(blocks, *cur)
			}
			cur = &Block{Kind: kind, Start: it.start, Count: uint16(it.end - int(it.start)), Tags: []int{it.idx}}
			curEnd = it.end
		}
		if cur != nil {
			blocks = append(blocks, *cur)
		}
	}
	return blocks
}
