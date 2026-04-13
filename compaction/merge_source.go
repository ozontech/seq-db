package compaction

import (
	"bytes"
	"iter"
	"math"
	"slices"

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
	info     *common.Info

	// sources is a slice of [sealing.Source]
	// which provide view into underlying fractions.
	sources []Source

	// docBlockCount is populated during [MergeSource.BlockOffsets] call.
	// This slice is used for changing block indexes in [seq.DocPos].
	docBlockCount []int

	// lidMapping describes the transformation of lids
	// after k-merge of several fractions.
	//
	// i-th index of [lidMapping] correponds to i-th fraction.
	// j-th index of i-th [lidMapping] corresponds to rename of j-th lid.
	lidMapping [][]uint32

	from, to seq.MID
}

func NewMergeSource(filename string, sources []Source) *MergeSource {
	lidmapping := make([][]uint32, len(sources))

	for i, src := range sources {
		lidmapping[i] = make([]uint32, src.Info().DocsTotal+1)
	}

	info := common.NewInfo(filename, 0, 0)
	info.SealingTime = info.CreationTime

	return &MergeSource{
		info:     info,
		filename: filename,

		sources:    sources,
		lidMapping: lidmapping,

		from: math.MaxUint64, to: 0,
	}
}

func (s *MergeSource) Info() *common.Info {
	for i := range s.sources {
		sinfo := s.sources[i].Info()

		s.info.DocsRaw += sinfo.DocsRaw
		s.info.DocsTotal += sinfo.DocsTotal
		s.info.DocsOnDisk += sinfo.DocsOnDisk

		// NOTE(dkharms): [IndexOnDisk] is calculated later.
	}

	s.info.From = s.from
	s.info.To = s.to

	return s.info
}

func (s *MergeSource) BlockOffsets() []uint64 {
	var (
		docsSize uint64
		offsets  []uint64
	)

	// Initially s.docBlockCount
	s.docBlockCount = append(s.docBlockCount, 0)
	for i := 0; i < len(s.sources); i++ {
		for _, offset := range s.sources[i].BlockOffsets() {
			offsets = append(offsets, uint64(offset)+docsSize)
		}
		docsSize += s.sources[i].Info().DocsOnDisk
		s.docBlockCount = append(s.docBlockCount, len(offsets))
	}

	return offsets
}

func (s *MergeSource) ID() iter.Seq2[seq.ID, seq.DocPos] {
	// TODO(dkharms): For now, I will use stupid-simple linear scan for k-way merge.
	//
	// Its time complexity O(k*n) so it's not efficient enough if we compare it
	// against time complexity of min-heap (which is O(n*log(k)))
	// or another great data structure -- tournament tree -- which is O(n*log(k)) as well.
	//
	// However, tournament tree performs less comparisons than min-heap
	// and it is around log(k) vs 2*log(k).

	type cursor struct {
		next func() (seq.ID, seq.DocPos, bool)
		stop func()

		id     seq.ID
		docPos seq.DocPos
		lidOld uint32

		ok bool
	}

	return func(yield func(seq.ID, seq.DocPos) bool) {
		var cursors []cursor

		for i := range s.sources {
			src := s.sources[i]
			next, stop := iter.Pull2(src.ID())

			// Skip [seq.SystemID] and [seq.SystemDocPos].
			_, _, _ = next()

			id, docpos, ok := next()
			cursors = append(cursors, cursor{
				next: next, stop: stop,
				id: id, docPos: docpos, lidOld: 1,
				ok: ok,
			})
		}

		defer func() {
			for _, c := range cursors {
				c.stop()
			}
		}()

		lid := uint32(1)
		// We've previosly dropped [seq.SystemID] from
		// iterators however we do have to emit one such id.
		if !yield(seq.SystemID, seq.SystemDocPos) {
			return
		}

		for {
			var (
				id  seq.ID = seq.MinID
				idx int    = -1
			)

			for i, c := range cursors {
				// We exhausted i-th cursor so there is nothing pull.
				if !c.ok {
					continue
				}

				if seq.Less(id, c.id) {
					id = c.id
					idx = i
				}
			}

			// All pull-iterators are exhausted.
			// Close all iterators and return.
			if idx == -1 {
				break
			}

			c := cursors[idx]
			minid, mindocpos, oldlid := c.id, c.docPos, c.lidOld

			blockIdx, offset := mindocpos.Unpack()
			mindocpos = seq.PackDocPos(uint32(s.docBlockCount[idx]+int(blockIdx)), offset)

			if !yield(minid, mindocpos) {
				return
			}

			// Rename lid from picked cursor to the new value.
			s.lidMapping[idx][oldlid] = lid

			c.id, c.docPos, c.ok = c.next()
			c.lidOld += 1

			s.from = min(s.from, minid.MID)
			s.to = max(s.to, minid.MID)

			lid += 1
			cursors[idx] = c
		}
	}
}

func (s *MergeSource) TokenTriplet() iter.Seq2[string, iter.Seq2[[]byte, []uint32]] {
	// TODO(dkharms): For now, I will use stupid-simple linear scan for k-way merge.
	//
	// Its time complexity O(k*n) so it's not efficient enough if we compare it
	// against time complexity of min-heap (which is O(n*log(k)))
	// or another great data structure -- tournament tree -- which is O(n*log(k)) as well.
	//
	// However, tournament tree performs less comparisons than min-heap
	// and it is around log(k) vs 2*log(k).

	type cursor struct {
		next func() (string, iter.Seq2[[]byte, []uint32], bool)
		stop func()

		field string
		tokIt iter.Seq2[[]byte, []uint32]

		ok bool
	}

	minimal := func(cursors []cursor) (string, bool) {
		var (
			set   bool
			field string
		)

		for _, c := range cursors {
			if !c.ok {
				continue
			}

			if !set {
				field = c.field
				set = true
				continue
			}

			field = min(field, c.field)
		}

		return field, set
	}

	return func(yield func(string, iter.Seq2[[]byte, []uint32]) bool) {
		var cursors []cursor

		for i := range s.sources {
			src := s.sources[i]

			next, stop := iter.Pull2(src.TokenTriplet())
			field, tokIt, has := next()

			cursors = append(cursors, cursor{
				next: next, stop: stop,
				field: field, tokIt: tokIt,
				ok: has,
			})
		}

		defer func() {
			for _, c := range cursors {
				c.stop()
			}
		}()

		for {
			field, ok := minimal(cursors)
			if !ok {
				break
			}

			var (
				idxs  []int
				iters []iter.Seq2[[]byte, []uint32]
			)

			for i, c := range cursors {
				if !c.ok || c.field != field {
					continue
				}

				idxs = append(idxs, i)
				iters = append(iters, c.tokIt)
			}

			if !yield(field, s.tokensForField(idxs, iters)) {
				return
			}

			// Advance all cursors that were on this field.
			for _, idx := range idxs {
				c := cursors[idx]
				c.field, c.tokIt, c.ok = c.next()
				cursors[idx] = c
			}
		}
	}
}

func (s *MergeSource) tokensForField(
	idxs []int, iters []iter.Seq2[[]byte, []uint32],
) iter.Seq2[[]byte, []uint32] {
	type cursor struct {
		next func() ([]byte, []uint32, bool)
		stop func()

		idx   int
		token []byte
		lids  []uint32

		ok bool
	}

	minimal := func(cursors []cursor) ([]byte, bool) {
		var (
			set   bool
			token []byte
		)

		for _, c := range cursors {
			if !c.ok {
				continue
			}

			if !set {
				token = c.token
				set = true
				continue
			}

			if bytes.Compare(c.token, token) < 0 {
				token = c.token
			}
		}

		return token, set
	}

	// NB: This buffer will be reused across
	// all calls within current field.
	var lidRenamed []uint32

	return func(yield func([]byte, []uint32) bool) {
		var cursors []cursor

		for i := range iters {
			next, stop := iter.Pull2(iters[i])
			token, lids, ok := next()
			cursors = append(cursors, cursor{
				next: next, stop: stop,
				idx: idxs[i], token: token, lids: lids,
				ok: ok,
			})
		}

		defer func() {
			for _, c := range cursors {
				c.stop()
			}
		}()

		for {
			token, ok := minimal(cursors)
			if !ok {
				break
			}

			// Collect and remap lids from all cursors at this token, then advance them.
			for i, c := range cursors {
				if !c.ok || !bytes.Equal(c.token, token) {
					continue
				}

				for _, lid := range c.lids {
					lidRenamed = append(lidRenamed, s.lidMapping[c.idx][lid])
				}

				c.token, c.lids, c.ok = c.next()
				cursors[i] = c
			}

			slices.Sort(lidRenamed)
			if !yield(token, lidRenamed) {
				return
			}

			lidRenamed = lidRenamed[:0]
		}
	}
}

func (s *MergeSource) LastError() error {
	return nil
}
