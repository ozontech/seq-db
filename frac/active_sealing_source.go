package frac

import (
	"bytes"
	"encoding/binary"
	"io"
	"iter"
	"os"
	"slices"
	"time"
	"unsafe"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type ActiveSealingSource struct {
	params        common.SealParams
	info          *common.Info
	created       time.Time
	sortedLIDs    []uint32
	oldToNewLIDs  []uint32
	mids          *UInt64s
	rids          *UInt64s
	fields        []string
	fieldsMaxTIDs []uint32
	tids          []uint32
	tokens        [][]byte
	lids          []*TokenLIDs
	docPosOrig    []seq.DocPos
	docPosSorted  []seq.DocPos
	blocksOffsets []uint64
	docsReader    *disk.DocsReader
	lastErr       error
}

func NewActiveSealingSource(active *Active, params common.SealParams) (*ActiveSealingSource, error) {
	info := *active.info // copy
	sortedLIDs := active.GetAllDocuments()
	sortedFields, fieldsMaxTIDs := sortFields(active.TokenList)
	sortedTIDs := sortTokens(sortedFields, active.TokenList)

	src := ActiveSealingSource{
		params:        params,
		info:          &info,
		created:       time.Now(),
		sortedLIDs:    sortedLIDs,
		oldToNewLIDs:  makeInverser(sortedLIDs),
		mids:          active.MIDs,
		rids:          active.RIDs,
		fields:        sortedFields,
		tids:          sortedTIDs,
		fieldsMaxTIDs: fieldsMaxTIDs,
		tokens:        active.TokenList.tidToVal,
		lids:          active.TokenList.tidToLIDs,
		docPosOrig:    active.DocsPositions.lidToPos,
		blocksOffsets: active.DocBlocks.vals,
		docsReader:    &active.sortReader,
	}

	src.prepareInfo()

	if !active.Config.SkipSortDocs {
		if err := src.SortDocs(); err != nil {
			return nil, err
		}
	}

	return &src, nil
}

func sortFields(tl *TokenList) ([]string, []uint32) {
	fields := make([]string, 0, len(tl.FieldTIDs))
	for field := range tl.FieldTIDs {
		fields = append(fields, field)
	}
	slices.Sort(fields)

	pos := 0
	maxTIDs := make([]uint32, 0, len(fields))
	for _, field := range fields {
		pos += len(tl.FieldTIDs[field])
		maxTIDs = append(maxTIDs, uint32(pos))
	}

	return fields, maxTIDs
}

func sortTokens(sortedFields []string, tl *TokenList) []uint32 {
	pos := 0
	tids := make([]uint32, 0, len(tl.tidToVal))
	for _, field := range sortedFields {
		tids = append(tids, tl.FieldTIDs[field]...)
		chunk := tids[pos:]
		slices.SortFunc(chunk, func(i, j uint32) int {
			a := tl.tidToVal[i]
			b := tl.tidToVal[j]
			return bytes.Compare(a, b)
		})
		pos = len(tids)
	}
	return tids
}

func (src *ActiveSealingSource) LastError() error {
	return src.lastErr
}

func (src *ActiveSealingSource) prepareInfo() {
	src.info.MetaOnDisk = 0
	src.info.SealingTime = uint64(src.created.UnixMilli())
	src.info.BuildDistribution(func(yield func(seq.ID) bool) {
		for i, mid := range src.mids.vals {
			if !yield(seq.ID{MID: seq.MID(mid), RID: seq.RID(src.rids.vals[i])}) {
				return
			}
		}
	})
}

func (src *ActiveSealingSource) Info() *common.Info {
	return src.info
}

func (src *ActiveSealingSource) TokenBlocks(blockSize int) iter.Seq[[][]byte] {
	const uint32Size = int(unsafe.Sizeof(uint32(0)))
	return func(yield func([][]byte) bool) {
		actualSize := 0
		block := make([][]byte, 0, blockSize)
		for _, tid := range src.tids {
			if actualSize >= blockSize {
				if !yield(block) {
					return
				}
				actualSize = 0
				block = block[:0]
			}
			token := src.tokens[tid]
			actualSize += len(token) + uint32Size
			block = append(block, token)
		}
		yield(block)
	}
}

func (src *ActiveSealingSource) Fields() iter.Seq2[string, uint32] {
	return func(yield func(string, uint32) bool) {
		for i, field := range src.fields {
			if !yield(field, src.fieldsMaxTIDs[i]) {
				return
			}
		}
	}
}

func (src *ActiveSealingSource) IDsBlocks(blockSize int) iter.Seq2[[]seq.ID, []seq.DocPos] {
	return func(yield func([]seq.ID, []seq.DocPos) bool) {
		mids := src.mids.vals
		rids := src.rids.vals

		ids := make([]seq.ID, 0, blockSize)
		pos := make([]seq.DocPos, 0, blockSize)

		// first
		ids = append(ids, seq.ID{MID: seq.MID(mids[0]), RID: seq.RID(rids[0])})
		pos = append(pos, 0)

		for i, lid := range src.sortedLIDs {
			if len(ids) == blockSize {
				if !yield(ids, pos) {
					return
				}
				ids = ids[:0]
				pos = pos[:0]
			}
			id := seq.ID{MID: seq.MID(mids[lid]), RID: seq.RID(rids[lid])}
			ids = append(ids, id)
			if len(src.docPosSorted) == 0 {
				pos = append(pos, src.docPosOrig[lid])
			} else {
				pos = append(pos, src.docPosSorted[i+1])
			}
		}
		yield(ids, pos)
	}
}

func (src *ActiveSealingSource) BlocksOffsets() []uint64 {
	return src.blocksOffsets
}

func (src *ActiveSealingSource) TokenLIDs() iter.Seq[[]uint32] {
	return func(yield func([]uint32) bool) {
		newLIDs := []uint32{}
		for _, tid := range src.tids {
			oldLIDs := src.lids[tid].GetLIDs(src.mids, src.rids)
			newLIDs = slices.Grow(newLIDs[:0], len(oldLIDs))
			for _, lid := range oldLIDs {
				newLIDs = append(newLIDs, src.oldToNewLIDs[lid])
			}
			if !yield(newLIDs) {
				return
			}
		}
	}
}

func makeInverser(sortedLIDs []uint32) []uint32 {
	inverser := make([]uint32, len(sortedLIDs)+1)
	for i, lid := range sortedLIDs {
		inverser[lid] = uint32(i + 1)
	}
	return inverser
}

func (src *ActiveSealingSource) Docs() iter.Seq2[seq.ID, []byte] {
	src.lastErr = nil
	return func(yield func(seq.ID, []byte) bool) {
		var (
			prev   seq.ID
			curDoc []byte
		)
		for ids, pos := range src.IDsBlocks(consts.IDsPerBlock) {
			for i, id := range ids {
				if id == systemSeqID {
					curDoc = nil
				} else if id != prev { // IDs may be repeated in case of nested index
					if curDoc, src.lastErr = src.doc(pos[i]); src.lastErr != nil {
						return
					}
				}
				prev = id
				if !yield(id, curDoc) {
					return
				}
			}
		}
	}
}

func (src *ActiveSealingSource) doc(pos seq.DocPos) ([]byte, error) {
	blockIndex, docOffset := pos.Unpack()
	blockOffset := src.blocksOffsets[blockIndex]

	var doc []byte
	err := src.docsReader.ReadDocsFunc(blockOffset, []uint64{docOffset}, func(b []byte) error {
		doc = b
		return nil
	})
	if err != nil {
		return nil, err
	}
	return doc, nil
}

func (src *ActiveSealingSource) SortDocs() error {
	start := time.Now()
	logger.Info("sorting docs...")
	sdocsFile, err := os.Create(src.info.Path + consts.SdocsTmpFileSuffix)
	if err != nil {
		return err
	}

	bw := bytespool.AcquireWriterSize(sdocsFile, consts.MB)
	defer bytespool.FlushReleaseWriter(bw)

	blocks := docBlocks(src.Docs(), src.params.DocBlockSize)
	blocksOffsets, positions, err := src.writeDocs(blocks, bw)

	if err = util.CollapseErrors([]error{src.lastErr, err}); err != nil {
		return err
	}

	bw.Flush()

	src.docPosSorted = positions
	src.blocksOffsets = blocksOffsets

	stat, err := sdocsFile.Stat()
	if err != nil {
		return err
	}
	src.info.DocsOnDisk = uint64(stat.Size())

	if err := sdocsFile.Sync(); err != nil {
		return err
	}
	if err := os.Rename(sdocsFile.Name(), src.info.Path+consts.SdocsFileSuffix); err != nil {
		return err
	}
	if err := sdocsFile.Close(); err != nil {
		return err
	}

	ratio := float64(src.info.DocsRaw) / float64(src.info.DocsOnDisk)
	logger.Info("docs sorting stat",
		util.ZapUint64AsSizeStr("raw", src.info.DocsRaw),
		util.ZapUint64AsSizeStr("compressed", src.info.DocsOnDisk),
		util.ZapFloat64WithPrec("ratio", ratio, 2),
		zap.Int("blocks_count", len(blocksOffsets)),
		zap.Int("docs_total", len(positions)),
		util.ZapDurationWithPrec("write_duration_ms", time.Since(start), "ms", 0),
	)

	return nil
}

// writeDocs compress and writes each block from DocBlock iterator
// and calc new blocks offsets and collects all docs positions
func (src *ActiveSealingSource) writeDocs(blocks iter.Seq2[[]byte, []seq.DocPos], w io.Writer) ([]uint64, []seq.DocPos, error) {
	offset := 0
	buf := make([]byte, 0)
	blocksOffsets := make([]uint64, 0)
	allPositions := make([]seq.DocPos, 0, len(src.mids.vals))

	for block, positions := range blocks {
		allPositions = append(allPositions, positions...)
		blocksOffsets = append(blocksOffsets, uint64(offset))
		buf = disk.CompressDocBlock(block, buf[:0], src.params.DocBlocksZstdLevel)
		if _, err := w.Write(buf); err != nil {
			return nil, nil, err
		}
		offset += len(buf)
	}
	return blocksOffsets, allPositions, nil
}

// docBlocks groups documents into blocks from an input iterator
// and returns an iterator of blocks and the document positions for each block
func docBlocks(docs iter.Seq2[seq.ID, []byte], blockSize int) iter.Seq2[[]byte, []seq.DocPos] {
	return func(yield func([]byte, []seq.DocPos) bool) {
		var (
			prev  seq.ID
			index uint32
		)
		pos := make([]seq.DocPos, 0)
		buf := make([]byte, 0, blockSize)
		for id, doc := range docs {
			if id == prev {
				// IDs have duplicates in case of nested index.
				// In this case we need to store the original document only once.
				// But store position for each LID
				pos = append(pos, seq.PackDocPos(index, uint64(len(buf))))
				continue
			}
			prev = id

			if len(buf) >= blockSize {
				if !yield(buf, pos) {
					return
				}
				index++
				buf = buf[:0]
				pos = pos[:0]
			}
			pos = append(pos, seq.PackDocPos(index, uint64(len(buf))))
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(doc)))
			buf = append(buf, doc...)
		}
		yield(buf, pos)
	}
}
