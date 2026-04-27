package compaction

import (
	"bytes"
	"iter"
	"slices"
	"sync"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/indexwriter"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type (
	Document         = util.Pair[seq.ID, []byte]
	DocBlockLocation = util.Pair[[]byte, uint64]
	TokenPosting     = util.Pair[[]byte, []uint32]
	DocLocation      = util.Pair[seq.ID, seq.DocPos]
	IndexedDocBlock  = util.Pair[[]byte, []seq.DocPos]
)

type Source interface {
	indexwriter.Source
	DocBlock() iter.Seq2[DocBlockLocation, error]
}

type MergeSource struct {
	filename string

	info     *common.Info
	infoOnce sync.Once

	// sources is a slice of [sealing.Source]
	// which provide view into underlying fractions.
	sources []Source

	offsets     []uint64
	offsetsOnce sync.Once
	// docBlockCount is populated during [MergeSource.BlockOffsets] call.
	// This slice is used for changing block indexes in [seq.DocPos].
	docBlockCount []int

	// lidMapping describes the transformation of lids
	// after k-merge of several fractions.
	//
	// i-th index of [lidMapping] correponds to i-th fraction.
	// j-th index of i-th [lidMapping] corresponds to rename of j-th lid.
	lidMapping [][]uint32
}

func NewMergeSource(filename string, sources []Source) *MergeSource {
	lidmapping := make([][]uint32, len(sources))

	for i, src := range sources {
		lidmapping[i] = make([]uint32, src.Info().DocsTotal+1)
	}

	s := &MergeSource{
		filename:   filename,
		sources:    sources,
		lidMapping: lidmapping,
	}

	s.info = s.prepareInfo()
	return s
}

func (s *MergeSource) prepareInfo() *common.Info {
	info := common.NewInfo(s.filename, 0, 0)

	var (
		from seq.MID = seq.MaxID.MID
		to   seq.MID = seq.MinID.MID
	)

	for _, src := range s.sources {
		from = min(from, src.Info().From)
		to = max(to, src.Info().To)
	}

	info.From, info.To = from, to
	info.SealingTime = info.CreationTime

	info.InitEmptyDistribution()
	return info
}

func (s *MergeSource) Info() *common.Info {
	s.infoOnce.Do(func() {
		for i := range s.sources {
			sinfo := s.sources[i].Info()

			s.info.DocsRaw += sinfo.DocsRaw
			s.info.DocsTotal += sinfo.DocsTotal
			s.info.DocsOnDisk += sinfo.DocsOnDisk

			// NOTE(dkharms): [IndexOnDisk] is calculated later.
		}
	})

	return s.info
}

func (s *MergeSource) BlockOffsets() []uint64 {
	s.offsetsOnce.Do(func() {
		var (
			docsSize uint64
			offsets  []uint64
		)

		s.docBlockCount = append(s.docBlockCount, 0)
		for i := 0; i < len(s.sources); i++ {
			for _, offset := range s.sources[i].BlockOffsets() {
				offsets = append(offsets, uint64(offset)+docsSize)
			}
			docsSize += s.sources[i].Info().DocsOnDisk
			s.docBlockCount = append(s.docBlockCount, len(offsets))
		}

		s.offsets = offsets
	})

	return s.offsets
}

func (s *MergeSource) ID() iter.Seq2[DocLocation, error] {
	// TODO(dkharms): For now, I will use stupid-simple linear scan for k-way merge.
	//
	// Its time complexity O(k*n) so it's not efficient enough if we compare it
	// against time complexity of min-heap (which is O(n*log(k)))
	// or another great data structure -- tournament tree -- which is O(n*log(k)) as well.
	//
	// However, tournament tree performs less comparisons than min-heap
	// and it is around log(k) vs 2*log(k).

	type cursor struct {
		next func() (DocLocation, error, bool)
		stop func()

		loc    DocLocation
		lidOld uint32

		ok bool
	}

	return func(yield func(DocLocation, error) bool) {
		var cursors []cursor

		defer func() {
			for _, c := range cursors {
				c.stop()
			}
		}()

		for i := range s.sources {
			src := s.sources[i]
			next, stop := iter.Pull2(src.ID())

			// Skip [seq.SystemID] and [seq.SystemDocPos].
			_, _, _ = next()

			loc, err, ok := next()
			cursors = append(cursors, cursor{
				next: next, stop: stop,
				loc: loc, lidOld: 1,
				ok: ok && err == nil,
			})

			if err != nil {
				yield(DocLocation{}, err)
				return
			}
		}

		lid := uint32(1)
		// We've previosly dropped [seq.SystemID] from
		// iterators however we do have to emit one such id.
		if !yield(DocLocation{First: seq.SystemID, Second: seq.SystemDocPos}, nil) {
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

				if seq.Less(id, c.loc.First) {
					id = c.loc.First
					idx = i
				}
			}

			// All pull-iterators are exhausted.
			// Close all iterators and return.
			if idx == -1 {
				break
			}

			c := cursors[idx]

			minid, oldlid := c.loc.First, c.lidOld
			s.info.AddMID(uint64(minid.MID))

			blockIdx, offset := c.loc.Second.Unpack()
			mindocpos := seq.PackDocPos(uint32(s.docBlockCount[idx]+int(blockIdx)), offset)

			if !yield(DocLocation{First: minid, Second: mindocpos}, nil) {
				return
			}

			// Rename lid from picked cursor to the new value.
			s.lidMapping[idx][oldlid] = lid

			var err error
			c.loc, err, c.ok = c.next()
			c.lidOld += 1

			if err != nil {
				cursors[idx] = c
				yield(DocLocation{}, err)
				return
			}

			lid += 1
			cursors[idx] = c
		}
	}
}

func (s *MergeSource) TokenTriplet() iter.Seq2[string, iter.Seq2[TokenPosting, error]] {
	// TODO(dkharms): For now, I will use stupid-simple linear scan for k-way merge.
	//
	// Its time complexity O(k*n) so it's not efficient enough if we compare it
	// against time complexity of min-heap (which is O(n*log(k)))
	// or another great data structure -- tournament tree -- which is O(n*log(k)) as well.
	//
	// However, tournament tree performs less comparisons than min-heap
	// and it is around log(k) vs 2*log(k).

	type cursor struct {
		next func() (string, iter.Seq2[TokenPosting, error], bool)
		stop func()

		field string
		tokIt iter.Seq2[TokenPosting, error]

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

	return func(yield func(string, iter.Seq2[TokenPosting, error]) bool) {
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
				iters []iter.Seq2[TokenPosting, error]
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
	idxs []int, iters []iter.Seq2[TokenPosting, error],
) iter.Seq2[TokenPosting, error] {
	type cursor struct {
		next func() (TokenPosting, error, bool)
		stop func()

		idx     int
		posting TokenPosting

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
				token = c.posting.First
				set = true
				continue
			}

			if bytes.Compare(c.posting.First, token) < 0 {
				token = c.posting.First
			}
		}

		return token, set
	}

	// NB: This buffer will be reused across
	// all calls within current field.
	var lidRenamed []uint32

	return func(yield func(TokenPosting, error) bool) {
		var cursors []cursor

		defer func() {
			for _, c := range cursors {
				c.stop()
			}
		}()

		for i := range iters {
			next, stop := iter.Pull2(iters[i])
			posting, err, ok := next()

			cursors = append(cursors, cursor{
				next: next, stop: stop,
				idx: idxs[i], posting: posting,
				ok: ok && err == nil,
			})

			if err != nil {
				yield(TokenPosting{}, err)
				return
			}
		}

		for {
			token, ok := minimal(cursors)
			if !ok {
				break
			}

			// Collect and remap lids from all cursors at this token, then advance them.
			for i, c := range cursors {
				if !c.ok || !bytes.Equal(c.posting.First, token) {
					continue
				}

				for _, lid := range c.posting.Second {
					lidRenamed = append(lidRenamed, s.lidMapping[c.idx][lid])
				}

				var err error
				c.posting, err, c.ok = c.next()

				if err != nil {
					cursors[i] = c
					yield(TokenPosting{}, err)
					return
				}

				cursors[i] = c
			}

			slices.Sort(lidRenamed)
			if !yield(TokenPosting{First: token, Second: lidRenamed}, nil) {
				return
			}

			lidRenamed = lidRenamed[:0]
		}
	}
}
