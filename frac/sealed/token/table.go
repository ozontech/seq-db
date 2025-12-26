package token

import (
	"sort"
	"strings"
	"unsafe"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

// token.Table maps fields to token.Blocks, specifying which block and the token range
// contains the field's token sequence.
//
// A single token.Block may contain tokens for multiple fields; thus, multiple
// token.TableEntry instances can reference the same block but different ranges.
//
// A single field may also span multiple token.Blocks entirely.
//
// Here's how it can be depicted:
//
// Field Ranges:    <-------f1----------><------f2-------><------------f3------------><----------f4---------->
// Token Blocks:    [.t1.t2.t3.t4.][.t5.t6.t7.t8.][.t9....etc...][.............][.............][.............]
// TableEntries:    {-----f1------}{-f1-}{---f2--}{--f2--}{-f3--}{------f3-----}{-f3-}{----f4-}{-----f4------}
//

const (
	TableEntrySize = unsafe.Sizeof(TableEntry{}) + unsafe.Sizeof(&TableEntry{})
	FieldDataSize  = unsafe.Sizeof(FieldData{}) + unsafe.Sizeof(&FieldData{})
)

type Table map[string]*FieldData

type FieldData struct {
	MinVal  string
	Entries []*TableEntry // expect that TableEntry are necessarily ordered by StartTID here
}

func cut(s string, l int) string {
	if len(s) > l {
		return s[:l]
	}
	return s
}

// SelectEntries returns monotonic and continuous sequence of token table entries
func (t Table) SelectEntries(field, hint string) []*TableEntry {
	data, ok := t[field]
	if !ok {
		return nil
	}

	if hint == "" { // fast path: return all field's entries
		return data.Entries
	}

	hintLen := len(hint)
	if hint < cut(data.MinVal, hintLen) { // we don't have a match
		return data.Entries[:0]
	}

	// we need to include next block after the last matching
	r := 1 + sort.Search(len(data.Entries)-1, func(i int) bool {
		return hint < cut(data.Entries[i].MaxVal, hintLen)
	})

	l := sort.Search(r, func(i int) bool {
		return hint <= cut(data.Entries[i].MaxVal, hintLen)
	})

	return data.Entries[l:r]
}

func (t Table) GetEntryByTID(tid uint32) *TableEntry {
	if tid == 0 {
		return nil
	}
	// todo: use bin search (we must have ordered slice here)
	for _, data := range t {
		for _, entry := range data.Entries {
			if tid >= entry.StartTID && tid < entry.StartTID+entry.ValCount {
				return entry
			}
		}
	}

	logger.Panic("can't find tid", zap.Uint32("tid", tid))
	return nil
}

// Size calculates a very approximate amount of memory occupied
func (t Table) Size() int {
	size := int(FieldDataSize) * len(t)
	for fieldName, fieldData := range t {
		size += len(fieldName) + len(fieldData.MinVal) + int(TableEntrySize)*len(fieldData.Entries)
		for _, e := range fieldData.Entries {
			size += len(e.MaxVal) + len(e.MinVal)
		}
	}
	return size
}

func (t Table) Clone() Table {
	res := make(Table, len(t))
	for k, v := range t {
		res[strings.Clone(k)] = v.Clone()
	}
	return res
}

func (fd *FieldData) Clone() *FieldData {
	res := FieldData{
		MinVal:  strings.Clone(fd.MinVal),
		Entries: make([]*TableEntry, 0, len(fd.Entries)),
	}
	for _, e := range fd.Entries {
		res.Entries = append(res.Entries, e.Clone())
	}
	return &res
}
