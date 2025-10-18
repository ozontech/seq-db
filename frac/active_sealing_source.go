package frac

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"time"
	"unsafe"

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

// ActiveSealingSource transforms data from in-memory (frac.Active) storage
// into a format suitable for disk writing during index creation.
//
// The main purpose of this type is to provide access to sorted data
// through a set of iterators that allow sequential processing of
// data in sized blocks for disk writing:
//
//   - TokenBlocks() - iterator for token blocks, sorted by fields and values
//   - Fields() - iterator for sorted fields with maximum TIDs
//   - IDsBlocks() - iterator for document ID blocks and their positions
//   - TokenLIDs() - iterator for LID lists for each token
//   - Docs() - iterator for documents themselves with duplicate handling
//
// All iterators work with pre-sorted data and return information
// in an order optimal for creating disk index structures.
type ActiveSealingSource struct {
	params        common.SealParams   // Sealing parameters
	info          *common.Info        // fraction Info
	created       time.Time           // Creation time of the source
	sortedLIDs    []uint32            // Sorted LIDs (Local ID)
	oldToNewLIDs  []uint32            // Mapping from old LIDs to new ones (after sorting)
	mids          *UInt64s            // MIDs
	rids          *UInt64s            // RIDs
	fields        []string            // Sorted field names
	fieldsMaxTIDs []uint32            // Maximum TIDs for each field
	tids          []uint32            // Sorted TIDs (Token ID)
	tokens        [][]byte            // Tokens (values) by TID
	lids          []*TokenLIDs        // LID lists for each token
	docPosOrig    []seq.DocPos        // Original document positions
	docPosSorted  []seq.DocPos        // Document positions after sorting
	blocksOffsets []uint64            // Document block offsets
	docsReader    *storage.DocsReader // Document storage reader
	lastErr       error               // Last error
}

// NewActiveSealingSource creates a new data source for sealing
// based on an active in-memory index.
func NewActiveSealingSource(active *Active, params common.SealParams) (*ActiveSealingSource, error) {
	info := *active.info // copy
	sortedLIDs := active.GetAllDocuments()

	// Sort fields and get maximum TIDs for each field
	sortedFields, fieldsMaxTIDs := sortFields(active.TokenList)

	// Sort tokens within each field
	sortedTIDs := sortTokens(sortedFields, active.TokenList)

	src := ActiveSealingSource{
		params:        params,
		info:          &info,
		created:       time.Now(),
		sortedLIDs:    sortedLIDs,
		oldToNewLIDs:  makeInverser(sortedLIDs), // Create LID mapping
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

	// Sort documents if not skipped in configuration
	if !active.Config.SkipSortDocs {
		if err := src.SortDocs(); err != nil {
			return nil, err
		}
	}

	return &src, nil
}

// sortFields sorts field names and calculates maximum TIDs for each field.
// Returns sorted field list and array of maximum TIDs.
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

// sortTokens sorts tokens lexicographically within each field.
// Returns sorted list of TIDs.
func sortTokens(sortedFields []string, tl *TokenList) []uint32 {
	pos := 0
	tids := make([]uint32, 0, len(tl.tidToVal))
	for _, field := range sortedFields {
		tids = append(tids, tl.FieldTIDs[field]...)
		chunk := tids[pos:]
		slices.SortFunc(chunk, func(i, j uint32) int {
			a := tl.tidToVal[i]
			b := tl.tidToVal[j]
			return bytes.Compare(a, b) // Sort by token value
		})
		pos = len(tids)
	}
	return tids
}

// LastError returns the last error that occurred during processing.
func (src *ActiveSealingSource) LastError() error {
	return src.lastErr
}

// prepareInfo prepares metadata for disk writing.
func (src *ActiveSealingSource) prepareInfo() {
	src.info.MetaOnDisk = 0
	src.info.SealingTime = uint64(src.created.UnixMilli())
	src.info.BuildDistribution(src.mids.vals)
}

// Info returns index metadata information.
func (src *ActiveSealingSource) Info() *common.Info {
	return src.info
}

// TokenBlocks returns an iterator for token blocks for disk writing.
// Tokens are pre-sorted: first by fields, then lexicographically within each field.
// Each block contains up to blockSize bytes of data for efficient writing.
func (src *ActiveSealingSource) TokenBlocks(blockSize int) iter.Seq[[][]byte] {
	const tokenLengthSize = int(unsafe.Sizeof(uint32(0)))
	return func(yield func([][]byte) bool) {
		if len(src.tids) == 0 {
			return
		}
		if blockSize <= 0 {
			src.lastErr = errors.New("sealing: token block size must be > 0")
			return
		}

		actualSize := 0
		block := make([][]byte, 0, blockSize)

		// Iterate through all sorted TIDs
		for _, tid := range src.tids {
			if actualSize >= blockSize {
				if !yield(block) {
					return
				}
				actualSize = 0
				block = block[:0]
			}
			token := src.tokens[tid]
			actualSize += tokenLengthSize // Add the size of the token length field
			actualSize += len(token)      // Add the size of the token itself
			block = append(block, token)
		}
		yield(block)
	}
}

// Fields returns an iterator for sorted fields and their maximum TIDs.
// Fields are sorted lexicographically, ensuring predictable order
// when building disk index structures.
func (src *ActiveSealingSource) Fields() iter.Seq2[string, uint32] {
	return func(yield func(string, uint32) bool) {
		for i, field := range src.fields {
			if !yield(field, src.fieldsMaxTIDs[i]) {
				return
			}
		}
	}
}

// IDsBlocks returns an iterator for document ID blocks and corresponding positions.
// IDs are sorted. Block size is controlled by blockSize parameter for balance between
// performance and memory usage.
func (src *ActiveSealingSource) IDsBlocks(blockSize int) iter.Seq2[[]seq.ID, []seq.DocPos] {
	return func(yield func([]seq.ID, []seq.DocPos) bool) {
		mids := src.mids.vals
		rids := src.rids.vals

		ids := make([]seq.ID, 0, blockSize)
		pos := make([]seq.DocPos, 0, blockSize)

		// First reserved ID (system). This position is not used because Local IDs (LIDs) use 1-based indexing.
		ids = append(ids, seq.ID{MID: seq.MID(mids[0]), RID: seq.RID(rids[0])})
		pos = append(pos, 0)

		// Iterate through sorted LIDs
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

			// Use sorted or original positions
			if len(src.docPosSorted) == 0 {
				pos = append(pos, src.docPosOrig[lid])
			} else {
				pos = append(pos, src.docPosSorted[i+1]) // +1 for system document
			}
		}
		yield(ids, pos)
	}
}

// BlocksOffsets returns document block offsets.
func (src *ActiveSealingSource) BlocksOffsets() []uint64 {
	return src.blocksOffsets
}

// TokenLIDs returns an iterator for LID lists for each token.
// LIDs are converted to new numbering after document sorting.
// Each iterator call returns a list of documents containing a specific token,
// in sorted order.
func (src *ActiveSealingSource) TokenLIDs() iter.Seq[[]uint32] {
	return func(yield func([]uint32) bool) {
		newLIDs := []uint32{}

		// For each sorted TID
		for _, tid := range src.tids {
			// Get original LIDs for this token
			oldLIDs := src.lids[tid].GetLIDs(src.mids, src.rids)
			newLIDs = slices.Grow(newLIDs[:0], len(oldLIDs))

			// Convert old LIDs to new through mapping
			for _, lid := range oldLIDs {
				newLIDs = append(newLIDs, src.oldToNewLIDs[lid])
			}

			if !yield(newLIDs) {
				return
			}
		}
	}
}

// makeInverser creates an array for converting old LIDs to new ones.
// sortedLIDs[i] = oldLID -> inverser[oldLID] = i+1
func makeInverser(sortedLIDs []uint32) []uint32 {
	inverser := make([]uint32, len(sortedLIDs)+1)
	for i, lid := range sortedLIDs {
		inverser[lid] = uint32(i + 1) // +1 because 0 position is reserved and unused
	}
	return inverser
}

// Docs returns an iterator for documents with their IDs.
// Handles duplicate IDs (for nested indexes).
func (src *ActiveSealingSource) Docs() iter.Seq2[seq.ID, []byte] {
	src.lastErr = nil
	return func(yield func(seq.ID, []byte) bool) {
		var (
			prev   seq.ID
			curDoc []byte
		)

		// Iterate through ID and position blocks
		for ids, pos := range src.IDsBlocks(consts.IDsPerBlock) {
			for i, id := range ids {
				if id == systemSeqID {
					curDoc = nil // reserved system document (no payload)
				} else if id != prev {
					// If ID changed, read new document
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

// doc reads a document from storage by its position.
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

	if err := util.CollapseErrors([]error{src.lastErr, err}); err != nil {
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

	// Synchronize and rename file
	if err := sdocsFile.Sync(); err != nil {
		return err
	}
	if err := sdocsFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(sdocsFile.Name(), src.info.Path+consts.SdocsFileSuffix); err != nil {
		return err
	}
	if err := util.SyncPath(filepath.Dir(src.info.Path)); err != nil {
		return err
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
func (src *ActiveSealingSource) writeDocs(blocks iter.Seq2[[]byte, []seq.DocPos], w io.Writer) ([]uint64, []seq.DocPos, error) {
	offset := 0
	buf := make([]byte, 0)
	blocksOffsets := make([]uint64, 0)
	allPositions := make([]seq.DocPos, 0, len(src.mids.vals))

	// Process each document block
	for block, positions := range blocks {
		allPositions = append(allPositions, positions...)
		blocksOffsets = append(blocksOffsets, uint64(offset))

		// Compress document block
		buf = storage.CompressDocBlock(block, buf[:0], src.params.DocBlocksZstdLevel)
		if _, err := w.Write(buf); err != nil {
			return nil, nil, err
		}
		offset += len(buf)
	}
	return blocksOffsets, allPositions, nil
}

// docBlocks groups documents into fixed-size blocks.
// Returns an iterator for blocks and corresponding document positions.
func docBlocks(docs iter.Seq2[seq.ID, []byte], blockSize int) iter.Seq2[[]byte, []seq.DocPos] {
	return func(yield func([]byte, []seq.DocPos) bool) {
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
		for id, doc := range docs {
			if id == prev {
				// Duplicate IDs (for nested indexes) - store document once,
				// but create positions for each LID
				pos = append(pos, seq.PackDocPos(index, uint64(len(buf))))
				continue
			}
			prev = id

			// If block is full, yield it
			if len(buf) >= blockSize {
				if !yield(buf, pos) {
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
		yield(buf, pos)
	}
}
