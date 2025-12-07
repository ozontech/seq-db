package active2

/*
import (
	"iter"
	"time"
	"unsafe"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/sealed/sealing"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type SealingSource struct {
	info    *frac.Info
	created time.Time
	index   *memIndex
	lastErr error
}

func NewSealingSource(active *Active2, params common.SealParams) (sealing.Source, error) {
	info := *active.info // copy
	src := SealingSource{
		info:    &info,
		created: time.Now(),
		index:   active.indexes.MergeAll(),
	}
	src.prepareInfo()

	if !active.Config.SkipSortDocs {
		sortedSrc, err := frac.NewSortedSealingSource(&src, &active.sortReader, params)
		if err != nil {
			return nil, err
		}
		return sortedSrc, nil
	}

	return &src, nil
}

func (src *SealingSource) prepareInfo() {
	src.info.MetaOnDisk = 0
	src.info.SealingTime = uint64(src.created.UnixMilli())
	src.info.BuildDistribution(func(yield func(seq.ID) bool) {
		for _, id := range src.index.ids {
			if !yield(id) {
				return
			}
		}
	})
}

func (src *SealingSource) Info() *common.Info {
	return src.info
}

func (src *SealingSource) IDsBlocks(blockSize int) iter.Seq2[[]seq.ID, []seq.DocPos] {
	return func(yield func([]seq.ID, []seq.DocPos) bool) {

		ids := make([]seq.ID, 0, blockSize)
		pos := make([]seq.DocPos, 0, blockSize)

		// first
		ids = append(ids, frac.SystemSeqID) // todo; get rid of SystemSeqID in index format
		pos = append(pos, 0)

		for _, id := range src.index.ids {
			if len(ids) == blockSize {
				if !yield(ids, pos) {
					return
				}
				ids = ids[:0]
				pos = pos[:0]
			}
			ids = append(ids, id)
			pos = append(pos, src.index.positions[id])
		}
		yield(ids, pos)
	}
}

func (src *SealingSource) TokenBlocks(blockSize int) iter.Seq[[][]byte] {
	const uint32Size = int(unsafe.Sizeof(uint32(0)))
	return func(yield func([][]byte) bool) {
		actualSize := 0
		block := make([][]byte, 0, blockSize)
		for _, token := range src.index.tokens {
			if actualSize >= blockSize {
				if !yield(block) {
					return
				}
				actualSize = 0
				block = block[:0]
			}
			actualSize += len(token) + uint32Size
			block = append(block, token)
		}
		yield(block)
	}
}

func (src *SealingSource) Fields() iter.Seq2[string, uint32] {
	return func(yield func(string, uint32) bool) {
		for _, field := range src.index.fields {
			f := util.ByteToStringUnsafe(field)
			r := src.index.fieldsTokens[f]
			if !yield(f, r.start+r.count) {
				return
			}
		}
	}
}

func (src *SealingSource) TokenLIDs() iter.Seq[[]uint32] {
	return func(yield func([]uint32) bool) {
		for _, tokenLIDs := range src.index.tokenLIDs {
			if !yield(tokenLIDs) {
				return
			}
		}
	}
}

func (src *SealingSource) BlocksOffsets() []uint64 {
	return src.index.blocksOffsets
}

func (ss *SealingSource) LastError() error {
	return ss.lastErr
}
*/
