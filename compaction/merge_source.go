package compaction

import (
	"cmp"
	"iter"
	"slices"
	"strings"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/indexwriter"
	"github.com/ozontech/seq-db/seq"
)

type Source interface {
	indexwriter.Source
	DocBlock() iter.Seq[[]byte]
}

type MergeSource struct {
	filename string

	// sources is a slice of [sealing.Source]
	// which provide view into underlying fractions.
	sources []Source

	// docblockcount is populated during [MergeSource.BlockOffsets] call.
	// This slice is used for changing block indexes in [seq.DocPos].
	docblockcount []int

	// lidmapping describes the transformation of lids
	// after k-merge of several fractions.
	//
	// i-th index of lidmapping correponds to i-th fraction.
	// j-th index of i-th lidmapping corresponds to rename of i-th lid.
	lidmapping [][]uint32
}

func NewMergeSource(filename string, sources []Source) *MergeSource {
	lidmapping := make([][]uint32, len(sources))
	for i, src := range sources {
		lidmapping[i] = make([]uint32, src.Info().DocsTotal+1)
	}
	return &MergeSource{sources: sources, lidmapping: lidmapping}
}

// FIXME(dkharms): now this is just a placeholder.
// And info can be caculated after all merges.
func (s *MergeSource) Info() *common.Info {
	var (
		docsOnDisk  uint64
		indexOnDisk uint64
	)

	for i := range s.sources {
		docsOnDisk += s.sources[i].Info().DocsOnDisk
		indexOnDisk += s.sources[i].Info().IndexOnDisk
	}

	return common.NewInfo(s.filename, docsOnDisk, 0)
}

func (s *MergeSource) BlockOffsets() []uint64 {
	var (
		docsSize uint64
		offsets  []uint64
	)

	s.docblockcount = append(s.docblockcount, 0)
	for i := 0; i < len(s.sources); i++ {
		for _, offset := range s.sources[i].BlockOffsets() {
			offsets = append(offsets, uint64(offset)+docsSize)
		}
		docsSize += s.sources[i].Info().DocsOnDisk
		s.docblockcount = append(s.docblockcount, len(offsets))
	}

	return offsets
}

func (s *MergeSource) ID() iter.Seq2[seq.ID, seq.DocPos] {
	// FIXME(dkharms): For now, I will use stupid-simple linear scan for k-way merge.
	//
	// Its time complexity O(k*n) so it's not efficient enough if we compare it
	// against time complexity of min-heap (which is O(n*log(k)))
	// or another great data structure -- tournament tree -- which is O(n * log(k)) as well.
	//
	// However, tournament tree performs less comparisons than min-heap
	// and it is around log(k) vs 2*log(k).

	type entry struct {
		id     seq.ID
		docpos seq.DocPos

		sourceIdx int
		oldlid    uint32
	}

	var ids []entry
	for i := 0; i < len(s.sources); i++ {
		var lid uint32
		for id, docpos := range s.sources[i].ID() {
			// Skip system [seq.ID].
			if id == seq.SystemID {
				lid += 1
				continue
			}

			blockIdx, offset := docpos.Unpack()
			docpos = seq.PackDocPos(uint32(s.docblockcount[i]+int(blockIdx)), offset)
			ids = append(ids, entry{id, docpos, i, lid})

			lid += 1
		}
	}

	slices.SortFunc(ids, func(x, y entry) int {
		if x.id.MID == y.id.MID {
			return -cmp.Compare(x.id.RID, y.id.RID)
		}
		return -cmp.Compare(x.id.MID, y.id.MID)
	})

	for i, entry := range ids {
		s.lidmapping[entry.sourceIdx][entry.oldlid] = uint32(i + 1)
	}

	return func(yield func(seq.ID, seq.DocPos) bool) {
		// Emit system id since we skipped all such ids previously.
		if !yield(seq.SystemID, seq.SystemDocPos) {
			return
		}

		for _, v := range ids {
			if !yield(v.id, v.docpos) {
				return
			}
		}
	}
}

type key struct {
	field string
	token string
}

type value struct {
	idx  int
	lids []uint32
}

func (s *MergeSource) TokenTriplet() iter.Seq2[string, iter.Seq2[[]byte, []uint32]] {
	// TODO(dkharms): Use heap or other more efficient data structure.
	// For now, I'll just dump everything into one array.

	values := make(map[key][]value)
	for i := 0; i < len(s.sources); i++ {
		for field, tokIter := range s.sources[i].TokenTriplet() {
			for tok, lids := range tokIter {
				k := key{field, string(tok)}
				values[k] = append(values[k], value{i, slices.Clone(lids)})
			}
		}
	}

	var keys []key
	for k := range values {
		keys = append(keys, k)
	}

	slices.SortFunc(keys, func(x, y key) int {
		if x.field != y.field {
			return strings.Compare(x.field, y.field)
		}
		return strings.Compare(x.token, y.token)
	})

	return func(yield func(string, iter.Seq2[[]byte, []uint32]) bool) {
		var previous string
		for _, k := range keys {
			if k.field == previous {
				continue
			}

			if !yield(k.field, s.tokensForField(k.field, keys, values)) {
				return
			}

			previous = k.field
		}
	}
}

func (s *MergeSource) tokensForField(
	field string, keys []key, values map[key][]value,
) iter.Seq2[[]byte, []uint32] {
	var filtered []key
	for _, k := range keys {
		if k.field == field {
			filtered = append(filtered, k)
		}
	}

	return func(yield func([]byte, []uint32) bool) {
		for _, k := range filtered {
			var buf []uint32

			for _, v := range values[k] {
				for _, lid := range v.lids {
					buf = append(buf, s.lidmapping[v.idx][lid])
				}
			}

			slices.Sort(buf)
			if !yield([]byte(k.token), buf) {
				return
			}
		}
	}
}

func (s *MergeSource) LastError() error {
	return nil
}
