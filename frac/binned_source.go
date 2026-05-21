package frac

import (
	"fmt"
	"iter"
	"slices"
	"time"

	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/seq"
)

type BinnedSource struct {
	params common.SealParams

	info *common.Info

	blocksOffsets []uint64

	sortedLIDs   []uint32
	oldToNewLIDs []uint32

	mids *UInt64s
	rids *UInt64s

	fields    []string
	fieldTIDs [][]uint32

	tokens [][]byte
	lids   []*TokenLIDs

	docPos map[seq.ID]seq.DocPos
}

type entry struct {
	lids   []uint32
	writer *binDocWriter
}

func PerformTimestampBinning(
	a *Active, doSort bool,
	params common.SealParams,
	nameFunc func() string,
) ([]*BinnedSource, error) {
	mids := a.MIDs.GetVals()
	entries := make(map[time.Time]*entry)

	for _, lid := range a.GetAllDocuments() {
		t := seq.MID(mids[lid]).Time().Truncate(params.BinSize)
		if e, ok := entries[t]; ok {
			e.lids = append(e.lids, lid)
			continue
		}

		w, err := newBinDocWriter(nameFunc(), params)
		if err != nil {
			return nil, err
		}

		e := entry{writer: w, lids: []uint32{lid}}
		entries[t] = &e
	}

	reorganizeDocuments := binDocumentsUnsorted
	if doSort {
		reorganizeDocuments = binDocumentsSorted
	}

	if err := reorganizeDocuments(entries, a, params.BinSize); err != nil {
		return nil, err
	}

	fields, fieldTIDs := sortFields(a.TokenList)
	result := make([]*BinnedSource, 0, len(entries))
	for _, e := range entries {
		offsets, positions, docsOnDisk, err := e.writer.flush()
		if err != nil {
			return nil, err
		}

		result = append(result, NewBinnedSource(
			e.writer.path, a, params,
			e.lids, fields, fieldTIDs,
			offsets, positions, docsOnDisk,
		))
	}

	return result, nil
}

func binDocumentsSorted(entries map[time.Time]*entry, a *Active, binSize time.Duration) error {
	var (
		prev   seq.ID
		curdoc []byte
	)

	mids := a.MIDs.vals
	rids := a.RIDs.vals

	for _, lid := range a.GetAllDocuments() {
		id := seq.ID{
			MID: seq.MID(mids[lid]),
			RID: seq.RID(rids[lid]),
		}

		if id != prev {
			pos := a.DocsPositions.idToPos[id]

			blockIndex, docOffset := pos.Unpack()
			blockOffset := a.DocBlocks.vals[blockIndex]

			err := a.docsReader.ReadDocsFunc(
				blockOffset, []uint64{docOffset},
				func(b []byte) error { curdoc = b; return nil },
			)
			if err != nil {
				return err
			}

			prev = id
		}

		t := seq.MIDToTime(id.MID).Truncate(binSize)
		if err := entries[t].writer.append(id, curdoc); err != nil {
			return err
		}
	}

	return nil
}

func binDocumentsUnsorted(entries map[time.Time]*entry, a *Active, binSize time.Duration) error {
	mids := a.MIDs.vals
	rids := a.RIDs.vals

	idToPos := a.DocsPositions.idToPos
	blocks := make([][]seq.ID, len(a.DocBlocks.vals))

	for _, lid := range a.GetAllDocuments() {
		id := seq.ID{
			MID: seq.MID(mids[lid]),
			RID: seq.RID(rids[lid]),
		}

		blockIdx, _ := idToPos[id].Unpack()
		blocks[blockIdx] = append(blocks[blockIdx], id)
	}

	for blockIdx, ids := range blocks {
		docOffsets := make([]uint64, len(ids))
		blockOffset := a.DocBlocks.vals[blockIdx]

		for i := range ids {
			_, docOffset := idToPos[ids[i]].Unpack()
			docOffsets[i] = docOffset
		}

		docs, err := a.docsReader.ReadDocsUncached(blockOffset, docOffsets)
		if err != nil {
			return err
		}

		for i := range ids {
			id := ids[i]
			t := id.MID.Time().Truncate(binSize)

			if err := entries[t].writer.append(id, docs[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

func NewBinnedSource(
	path string, a *Active, params common.SealParams,
	lids []uint32, fields []string, fieldTIDs [][]uint32,
	blocksOffsets []uint64, docPos map[seq.ID]seq.DocPos, docsOnDisk uint64,
) *BinnedSource {
	b := &BinnedSource{
		params: params,

		info: common.NewInfo(path, 0, 0),

		sortedLIDs:    lids,
		blocksOffsets: blocksOffsets,
		docPos:        docPos,

		mids: a.MIDs,
		rids: a.RIDs,

		fields:    fields,
		fieldTIDs: fieldTIDs,
		tokens:    a.TokenList.tidToVal,
		lids:      a.TokenList.tidToLIDs,
	}

	b.info.DocsOnDisk = docsOnDisk
	b.oldToNewLIDs = make([]uint32, len(b.mids.vals))
	for i, lid := range lids {
		b.oldToNewLIDs[lid] = uint32(i + 1)
	}

	b.prepareLids()
	b.prepareInfo()

	return b
}

func (b *BinnedSource) prepareInfo() {
	b.info.MetaOnDisk = 0
	b.info.DocsTotal = uint32(len(b.sortedLIDs))
	b.info.SealingTime = uint64(time.Now().UnixMilli())

	if len(b.sortedLIDs) > 0 {
		b.info.To = seq.MID(b.mids.vals[b.sortedLIDs[0]])
		b.info.From = seq.MID(b.mids.vals[b.sortedLIDs[len(b.sortedLIDs)-1]])
	}

	mids := make([]uint64, len(b.sortedLIDs))
	for i, lid := range b.sortedLIDs {
		mids[i] = b.mids.vals[lid]
	}

	b.info.BuildDistribution(mids)
}

func (b *BinnedSource) prepareLids() {
	for _, tl := range b.lids[1:] {
		tl.GetLIDs(b.mids, b.rids)
	}
}

func (b *BinnedSource) Info() *common.Info {
	return b.info
}

func (b *BinnedSource) BlockOffsets() []uint64 {
	return b.blocksOffsets
}

func (b *BinnedSource) ID() iter.Seq2[DocLocation, error] {
	return func(yield func(DocLocation, error) bool) {
		mids := b.mids.vals
		rids := b.rids.vals

		if !yield(DocLocation{First: seq.SystemID, Second: seq.SystemDocPos}, nil) {
			return
		}

		for _, lid := range b.sortedLIDs {
			id := seq.ID{MID: seq.MID(mids[lid]), RID: seq.RID(rids[lid])}
			// TODO(dkharms): Validate correctness?
			dloc := DocLocation{First: id, Second: b.docPos[id]}
			if !yield(dloc, nil) {
				return
			}
		}
	}
}

func (b *BinnedSource) TokenTriplet() iter.Seq2[string, iter.Seq2[TokenPosting, error]] {
	return func(yield func(string, iter.Seq2[TokenPosting, error]) bool) {
		for idx, field := range b.fields {
			if !yield(field, b.postingsForField(field, idx)) {
				return
			}
		}
	}
}

func (b *BinnedSource) postingsForField(field string, idx int) iter.Seq2[TokenPosting, error] {
	var lidsbuf []uint32
	return func(yield func(TokenPosting, error) bool) {
		for _, tid := range b.fieldTIDs[idx] {
			token := b.tokens[tid]

			lids := b.lids[tid].SortedLIDsUnsafe()
			lidsbuf = slices.Grow(lidsbuf[:0], len(lids))

			for _, lid := range lids {
				if newLID := b.oldToNewLIDs[lid]; newLID != 0 {
					lidsbuf = append(lidsbuf, newLID)
				}
			}

			if field == "user_id" && string(token) == "10" {
				fmt.Printf("field: %v\n", field)
				fmt.Printf("token: %v\n", string(token))
				fmt.Printf("lidsbuf: %v\n", lidsbuf)
			}

			if len(lidsbuf) == 0 {
				continue
			}

			if !yield(TokenPosting{First: token, Second: lidsbuf}, nil) {
				return
			}
		}
	}
}
