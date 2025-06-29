package token

import (
	"sort"
	"unsafe"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

// `token.Table` describes the mapping of a fields to `token.Blocks`: in which block
// and in what range is the sequence of field's tokens located.
//
// One `token.Block` can contain tokens for multiple fields, and therefore multiple
// `token.TableEntry` instances can reference the same block but different ranges.
//
// Also, one field can completely occupy several `token.Block`s.
//
// Here's how it can be depicted:
//
//                              f1        f2  f3    f4              f5                    f6
// fields:       		<---------------><--><---><----><-------------------------><----------------->
// token.Block's: 		[...........][...........][...........][...........][...........][...........]
// token.TableEntry's:  {-----------}{--}{--}{---}{----}{-----}{-----------}{----}{-----}{-----------}
//                          f1        f1  f2   f3   f4     f5        f5       f5     f6        f6
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
