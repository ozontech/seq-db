package active

import (
	"iter"
	"math"
	"time"

	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/active_old"
	"github.com/ozontech/seq-db/frac/sealed/sealing"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

var (
	_ sealing.Source = (*SealingSource)(nil)

	// Special system ID added as the first entry
	systemSeqID = seq.ID{
		MID: math.MaxUint64,
		RID: math.MaxUint64,
	}
)

// SealingSource provides data from a single memIndex in the format required by the sealing stage.
type SealingSource struct {
	info    *frac.Info
	index   *memIndex
	lastErr error
}

// NewSealingSource prepares a sealing source from Active2 state.
func NewSealingSource(a *Active, params frac.SealParams) (sealing.Source, error) {
	a.merger.ForceMergeAll()

	iss, release := a.indexes.Snapshot()
	defer release()

	if len(iss.indexes) != 1 {
		logger.Panic("invalid state: sealing requires a single memIndex")
	}

	ss := &SealingSource{
		info:  iss.info,
		index: iss.indexes[0],
	}

	// Sort documents unless explicitly disabled
	if !a.Config.SkipSortDocs {
		ds := active_old.NewDocsSource(ss, ss.index.blocksOffsets, &a.sortReader)

		blocksOffsets, positions, onDiskSize, err := sealing.SortDocs(
			ss.info.Path,
			params,
			ds,
		)
		if err != nil {
			return nil, err
		}

		// Skip system document position
		ss.index.positions = positions[1:]
		ss.index.blocksOffsets = blocksOffsets
		ss.info.DocsOnDisk = uint64(onDiskSize)
	}

	ss.info.MetaOnDisk = 0
	ss.info.SealingTime = uint64(time.Now().UnixMilli())

	ss.info.BuildDistributionWithIDs(ss.index.ids)

	return ss, nil
}

// Info returns fraction metadata.
func (src *SealingSource) Info() *frac.Info {
	return src.info
}

// IDsBlocks yields document IDs and their positions in fixed-size blocks.
func (src *SealingSource) IDsBlocks(blockSize int) iter.Seq2[[]seq.ID, []seq.DocPos] {
	return func(yield func([]seq.ID, []seq.DocPos) bool) {
		ids := make([]seq.ID, 0, blockSize)
		pos := make([]seq.DocPos, 0, blockSize)

		// Add system entry
		ids = append(ids, systemSeqID)
		pos = append(pos, 0)

		for i, id := range src.index.ids {
			if len(ids) == blockSize {
				if !yield(ids, pos) {
					return
				}
				ids = ids[:0]
				pos = pos[:0]
			}

			ids = append(ids, id)
			pos = append(pos, src.index.positions[i])
		}

		yield(ids, pos)
	}
}

// TokenBlocks yields tokens grouped by total byte size.
func (src *SealingSource) TokenBlocks(blockSize int) iter.Seq[[][]byte] {
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

			actualSize += len(token) + int(uint32Size)
			block = append(block, token)
		}

		yield(block)
	}
}

// Fields yields field names with their token upper bounds.
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

// TokenLIDs yields document LIDs for each token.
func (src *SealingSource) TokenLIDs() iter.Seq[[]uint32] {
	all := make([]uint32, src.index.docsCount)
	for i := range all {
		all[i] = uint32(i) + 1
	}

	return func(yield func([]uint32) bool) {
		for _, tokenLIDs := range src.index.tokenLIDs {
			if len(tokenLIDs) == 0 {
				tokenLIDs = all
			}
			if !yield(tokenLIDs) {
				return
			}
		}
	}
}

// BlocksOffsets returns document block offsets.
func (src *SealingSource) BlocksOffsets() []uint64 {
	return src.index.blocksOffsets
}

// LastError returns the last recorded source error.
func (src *SealingSource) LastError() error {
	return src.lastErr
}
