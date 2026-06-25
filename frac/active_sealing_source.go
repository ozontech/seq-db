package frac

import (
	"bytes"
	"encoding/binary"
	"io"
	"iter"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/alecthomas/units"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

type (
	Document        = util.Pair[seq.ID, []byte]
	TokenPosting    = util.Pair[[]byte, []uint32]
	DocLocation     = util.Pair[seq.ID, seq.DocPos]
	IndexedDocBlock = util.Pair[[]byte, []seq.DocPos]
)

type ActiveSealingSource struct {
	params        common.SealParams     // Sealing parameters
	info          *common.Info          // fraction Info
	created       time.Time             // Creation time of the source
	sortedLIDs    []uint32              // Sorted LIDs (Local ID)
	oldToNewLIDs  []uint32              // Mapping from old LIDs to new ones (after sorting)
	mids          *UInt64s              // MIDs
	rids          *UInt64s              // RIDs
	fields        []string              // Sorted field names
	fieldsMaxTIDs []uint32              // Maximum TIDs for each field
	tids          []uint32              // Sorted TIDs (Token ID)
	tokens        [][]byte              // Tokens (values) by TID
	lids          []*TokenLIDs          // LID lists for each token
	docPosMap     map[seq.ID]seq.DocPos // Original document positions
	docPosSorted  []seq.DocPos          // Document positions after sorting
	blocksOffsets []uint64              // Document block offsets
	docsReader    *storage.DocsReader   // Document storage reader
	lastErr       error                 // Last error
	skipFsync     bool
}

func NewActiveSealingSource(active *Active, params common.SealParams) (*ActiveSealingSource, error) {
	info := *active.info // copy

	sortedLIDs := active.GetAllDocuments()
	fields, fieldTIDs := sortFields(active.TokenList)

	src := ActiveSealingSource{
		params: params,

		info:    &info,
		created: time.Now(),

		sortedLIDs:   sortedLIDs,
		oldToNewLIDs: makeInverser(sortedLIDs), // Create LID mapping

		mids: active.MIDs,
		rids: active.RIDs,

		fields:    fields,
		fieldTIDs: fieldTIDs,
		tokens:    active.TokenList.tidToVal,
		lids:      active.TokenList.tidToLIDs,

		docPosMap:     active.DocsPositions.idToPos,
		blocksOffsets: active.DocBlocks.vals,
		docsReader:    &active.sortReader,
		skipFsync:     active.Config.SkipFsync,
	}

	src.prepareInfo()
	src.prepareLids()

	// Sort documents if not skipped in configuration
	if !active.Config.SkipSortDocs {
		if err := src.SortDocs(); err != nil {
			return nil, err
		}
	}

	return &src, nil
}

func sortFields(tl *TokenList) ([]string, [][]uint32) {
	fields := slices.Collect(maps.Keys(tl.FieldTIDs))
	slices.Sort(fields)

	fieldTIDs := make([][]uint32, len(tl.FieldTIDs))
	for i, field := range fields {
		// Make a copy because this memory is shared
		// with concurrent readers (user search queries).
		cp := slices.Clone(tl.FieldTIDs[field])

		slices.SortFunc(cp, func(i, j uint32) int {
			return bytes.Compare(tl.tidToVal[i], tl.tidToVal[j])
		})

		fieldTIDs[i] = cp
	}

	return fields, fieldTIDs
}

func (src *ActiveSealingSource) ID() iter.Seq2[DocLocation, error] {
	return func(yield func(DocLocation, error) bool) {
		mids := src.mids.vals
		rids := src.rids.vals

		// System ID and DocPos are not stored in `src.sortedLIDs`.
		// However we do have to yield them to preserve 1-based indexing for ids.
		dloc := DocLocation{First: seq.SystemID, Second: seq.SystemDocPos}
		if !yield(dloc, nil) {
			return
		}

		for i, lid := range src.sortedLIDs {
			dloc := DocLocation{
				First: seq.ID{
					MID: seq.MID(mids[lid]),
					RID: seq.RID(rids[lid]),
				},
			}

			// Documents were not sorted previously.
			if len(src.docPosSorted) == 0 {
				dloc.Second = src.docPosMap[dloc.First]
				if !yield(dloc, nil) {
					return
				}
				continue
			}

			// `i` in range [0; len(src.sortedLIDs))
			// but lids indexes are 1-based.
			dloc.Second = src.docPosSorted[i+1]
			if !yield(dloc, nil) {
				return
			}
		}
	}
}

func (src *ActiveSealingSource) BlockOffsets() []uint64 {
	return src.blocksOffsets
}

func (src *ActiveSealingSource) prepareInfo() {
	src.info.MetaOnDisk = 0
	src.info.SealingTime = uint64(src.created.UnixMilli())
	mids := src.mids.vals
	if len(mids) > 1 {
		// skip system MID
		mids = mids[1:]
	}
	src.info.BuildDistribution(mids)
}

func (src *ActiveSealingSource) prepareLids() {
	for _, tl := range src.lids[1:] {
		tl.GetLIDs(src.mids, src.rids)
	}
}

func (src *ActiveSealingSource) Info() *common.Info {
	return src.info
}

func (src *ActiveSealingSource) TokenTriplet() iter.Seq2[string, iter.Seq2[TokenPosting, error]] {
	return func(yield func(string, iter.Seq2[TokenPosting, error]) bool) {
		for idx, field := range src.fields {
			if !yield(field, src.postingsForField(field, idx)) {
				return
			}
		}
	}
}

func (src *ActiveSealingSource) postingsForField(field string, idx int) iter.Seq2[TokenPosting, error] {
	var lidsbuf []uint32
	return func(yield func(TokenPosting, error) bool) {
		for _, tid := range src.fieldTIDs[idx] {
			token := src.tokens[tid]

			lids := src.lids[tid].SortedLIDsUnsafe()
			lidsbuf = slices.Grow(lidsbuf[:0], len(lids))

			for _, lid := range lids {
				lidsbuf = append(lidsbuf, src.oldToNewLIDs[lid])
			}

			tpost := TokenPosting{First: token, Second: lidsbuf}
			if !yield(tpost, nil) {
				return
			}
		}
	}
}

func makeInverser(sortedLIDs []uint32) []uint32 {
	inverser := make([]uint32, len(sortedLIDs)+1)
	for i, lid := range sortedLIDs {
		inverser[lid] = uint32(i + 1) // +1 because 0 position is reserved and unused
	}
	return inverser
}

// Docs returns an iterator for documents with their IDs.
// Handles duplicate IDs (for nested indexes).
func (src *ActiveSealingSource) Docs() iter.Seq2[Document, error] {
	return func(yield func(Document, error) bool) {
		var (
			curdoc []byte
			prev   seq.ID = seq.SystemID
		)

		for dloc, err := range src.ID() {
			if err != nil {
				yield(Document{}, err)
				return
			}

			id, pos := dloc.First, dloc.Second

			if id != prev {
				xcurdoc, xerr := src.doc(pos)
				if xerr != nil {
					yield(Document{}, xerr)
					return
				}
				curdoc = xcurdoc
			}

			prev = id
			doc := Document{First: id, Second: curdoc}

			if !yield(doc, nil) {
				return
			}
		}
	}
}

// doc reads a document from storage by its position.
func (src *ActiveSealingSource) doc(pos seq.DocPos) ([]byte, error) {
	blockIndex, docOffset := pos.Unpack()
	blockOffset := src.blocksOffsets[blockIndex]

	var doc []byte
	err := src.docsReader.ReadDocsFunc(
		blockOffset, []uint64{docOffset},
		func(b []byte) error {
			doc = b
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// SortDocs sorts documents and writes them in compressed form to disk.
// Creates a temporary file that is then renamed to the final one.
func (src *ActiveSealingSource) SortDocs() error {
	start := time.Now()
	logger.Info("sorting docs...")

	// Create temporary file for sorted documents
	sdocsFile, err := os.Create(src.info.Path + consts.SdocsTmpFileSuffix)
	if err != nil {
		return err
	}

	bw := bytespool.AcquireWriterSize(sdocsFile, int(units.MiB))
	defer bytespool.ReleaseWriter(bw)

	// Group documents into blocks
	blocks := docBlocks(src.Docs(), src.params.DocBlockSize)

	// Write blocks and get new offsets and positions
	blocksOffsets, positions, err := src.writeDocs(blocks, bw)
	if err != nil {
		return err
	}

	if err := bw.Flush(); err != nil {
		return err
	}

	src.docPosSorted = positions
	src.blocksOffsets = blocksOffsets

	// Get file statistics
	stat, err := sdocsFile.Stat()
	if err != nil {
		return err
	}
	src.info.DocsOnDisk = uint64(stat.Size())

	if !src.skipFsync {
		if err := sdocsFile.Sync(); err != nil {
			return err
		}
	}

	if err := sdocsFile.Close(); err != nil {
		return err
	}

	if err := os.Rename(sdocsFile.Name(), src.info.Path+consts.SdocsFileSuffix); err != nil {
		return err
	}
	if !src.skipFsync {
		if err := util.SyncPath(filepath.Dir(src.info.Path)); err != nil {
			return err
		}
	}

	// Log compression statistics
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

// writeDocs compresses and writes document blocks, calculating new offsets
// and collecting document positions.
func (src *ActiveSealingSource) writeDocs(blocks iter.Seq2[IndexedDocBlock, error], w io.Writer) ([]uint64, []seq.DocPos, error) {
	offset := 0
	buf := make([]byte, 0)
	blocksOffsets := make([]uint64, 0)
	allPositions := make([]seq.DocPos, 0, len(src.mids.vals))

	// Process each document block
	for docBlock, err := range blocks {
		if err != nil {
			return nil, nil, err
		}

		allPositions = append(allPositions, docBlock.Second...)
		blocksOffsets = append(blocksOffsets, uint64(offset))

		// Compress document block
		buf = storage.CompressDocBlock(docBlock.First, buf[:0], src.params.DocBlocksZstdLevel)
		if _, err := w.Write(buf); err != nil {
			return nil, nil, err
		}

		offset += len(buf)
	}

	return blocksOffsets, allPositions, nil
}

// docBlocks groups documents into fixed-size blocks.
// Returns an iterator for blocks and corresponding document positions.
func docBlocks(docs iter.Seq2[Document, error], blockSize int) iter.Seq2[IndexedDocBlock, error] {
	return func(yield func(IndexedDocBlock, error) bool) {
		const defaultBlockSize = 128 * units.KiB

		if blockSize <= 0 {
			blockSize = int(defaultBlockSize)
			logger.Warn("document block size not specified", zap.Int("default_size", blockSize))
		}

		var (
			prev  seq.ID
			index uint32 // Current block index
		)

		pos := make([]seq.DocPos, 0)
		buf := make([]byte, 0, blockSize)

		// Iterate through documents
		for doc, err := range docs {
			if err != nil {
				yield(IndexedDocBlock{}, err)
				return
			}

			id, doc := doc.First, doc.Second
			if id == prev {
				// Duplicate IDs (for nested indexes) - store document once,
				// but create positions for each LID
				pos = append(pos, seq.PackDocPos(index, uint64(len(buf))))
				continue
			}

			prev = id

			// If block is full, yield it
			if len(buf) >= blockSize {
				docBlock := IndexedDocBlock{First: buf, Second: pos}
				if !yield(docBlock, nil) {
					return
				}

				index++
				buf = buf[:0]
				pos = pos[:0]
			}

			// Add document position
			pos = append(pos, seq.PackDocPos(index, uint64(len(buf))))

			// Write document size and the document itself
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(doc)))
			buf = append(buf, doc...)
		}

		docBlock := IndexedDocBlock{First: buf, Second: pos}
		yield(docBlock, nil)
	}
}
