package sealing

import (
	"bytes"
	"encoding/binary"
	"io"
	"iter"
	"time"

	"github.com/alecthomas/units"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/seqids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

// IndexSealer is responsible for creating and writing the index structure for sealed fractions.
// It organizes data into blocks, compresses them, and builds the complete index file with:
// - Multiple data sections (info, tokens, token table, offsets, IDs, LIDs)
// - Compression using ZSTD with configurable levels
// - Registry for quick access to block locations
// - PreloadedData structures for fast initialization instance of sealed fraction
type IndexSealer struct {
	lastErr error             // Last error encountered during processing
	buf1    []byte            // Reusable buffer for packing raw data before compression
	buf2    []byte            // Reusable buffer for compressed data
	params  common.SealParams // Configuration parameters for sealing process

	// PreloadedData structures built during sealing for fast initialization of sealed fraction
	idsTable   seqids.Table // Table mapping document IDs to blocks
	lidsTable  lids.Table   // Table mapping token IDs to LID blocks
	tokenTable token.Table  // Table mapping fields to token blocks
}

// NewIndexSealer creates a new IndexSealer instance with the given parameters.
func NewIndexSealer(params common.SealParams) *IndexSealer {
	return &IndexSealer{
		params: params,
		buf1:   make([]byte, 0, consts.RegularBlockSize),
		buf2:   make([]byte, 0, consts.RegularBlockSize),
	}
}

// indexBlock represents a single block of data in the index file.
// Each block can be compressed and contains metadata for efficient retrieval.
type indexBlock struct {
	codec   storage.Codec // Compression codec used (No compression or ZSTD)
	payload []byte        // The actual block data (may be compressed)
	rawLen  uint32        // Original uncompressed data length
	ext1    uint64        // Extended metadata field 1 (block-specific usage)
	ext2    uint64        // Extended metadata field 2 (block-specific usage)
}

// Bin converts the indexBlock to its binary representation for storage.
// It creates a header with metadata and returns the header + payload.
// Parameters:
//   - pos: The file position where this block will be written
//
// Returns:
//   - storage.IndexBlockHeader: The block header with metadata
//   - []byte: The payload data to write
func (i indexBlock) Bin(pos int64) (storage.IndexBlockHeader, []byte) {
	header := storage.NewIndexBlockHeader(pos, i.ext1, i.ext2, uint32(len(i.payload)), i.rawLen, i.codec)
	return header, i.payload
}

// WriteIndex writes the complete index structure to the provided writer.
// The index file structure:
// +----------------+----------------+----------------+
// | Prefix         | Data Blocks    | Registry       |
// | (16 bytes)     | (multiple)     | (block headers)|
// +----------------+----------------+----------------+
//
// Prefix contains:
// - 8 bytes: Position of registry start
// - 8 bytes: Size of registry
//
// Parameters:
//   - ws: WriteSeeker to write the index data to
//   - src: Source interface providing the data to be sealed
//
// Returns:
//   - error: Any error encountered during writing
func (s *IndexSealer) WriteIndex(ws io.WriteSeeker, src Source) error {
	const prefixSize = 16 // Size of prefix that will hold registry position and size

	// Skip prefix area initially - we'll write it at the end
	if _, err := ws.Seek(prefixSize, io.SeekStart); err != nil {
		return err
	}

	// Create buffers for headers and payload writing
	hw := bytes.NewBuffer(nil)                            // Headers writer - collects all block headers
	bw := bytespool.AcquireWriterSize(ws, int(units.MiB)) // Buffered writer for payload
	defer bytespool.ReleaseWriter(bw)

	// Write all index blocks and collect headers
	if err := s.writeBlocks(prefixSize, bw, hw, src); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}

	// Calculate registry position and size
	size := hw.Len()                   // Registry size (all headers)
	pos, err := ws.Seek(0, io.SeekEnd) // Current end position = registry start
	if err != nil {
		return err
	}

	// Write registry (all block headers) at the end of file
	if _, err := bw.Write(hw.Bytes()); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}

	// Write prefix at beginning of file with registry metadata
	prefix := make([]byte, 0, prefixSize)
	prefix = binary.LittleEndian.AppendUint64(prefix, uint64(pos))  // Registry position
	prefix = binary.LittleEndian.AppendUint64(prefix, uint64(size)) // Registry size
	if _, err := ws.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err = ws.Write(prefix); err != nil {
		return err
	}

	return nil
}

// writeBlocks processes all index blocks from the source and writes them to the output.
// It simultaneously writes payload data to one writer and headers to another.
// Parameters:
//   - pos: Starting position for the first block
//   - payloadWriter: Writer for block payload data
//   - headersWriter: Writer for block headers (registry)
//   - src: Data source
//
// Returns:
//   - error: Any error encountered during processing
func (s *IndexSealer) writeBlocks(pos int, payloadWriter, headersWriter io.Writer, src Source) error {
	// Process each index block from the source
	for block := range s.indexBlocks(src) {
		header, payload := block.Bin(int64(pos))
		// Write payload to main data section
		if _, err := payloadWriter.Write(payload); err != nil {
			return err
		}
		// Write header to registry
		if _, err := headersWriter.Write(header); err != nil {
			return err
		}
		pos += len(payload) // Advance position for next block
	}
	if s.lastErr != nil {
		return s.lastErr
	}
	return nil
}

// indexBlocks generates a sequence of index blocks from the source data.
// The blocks are organized in specific sections:
// 1. Info Section - Basic fraction metadata
// 2. Tokens Section - Token data blocks
// 3. Token Table Section - Field-to-token mapping table
// 4. Offsets Section - Document block offsets
// 5. IDs Section - Document ID blocks (MIDs, RIDs, Positions)
// 6. LIDs Section - Token ID to LID mapping blocks
//
// Returns:
//   - iter.Seq[indexBlock]: Sequence of index blocks to write
func (s *IndexSealer) indexBlocks(src Source) iter.Seq[indexBlock] {
	return func(yield func(indexBlock) bool) {
		bb := blocksBuilder{}
		blocksCounter := uint32(0)   // Global block counter for indexing
		statsOverall := startStats() // Overall statistics collector

		// Helper to push a block and update statistics
		push := func(b indexBlock, statsSection *blocksStats) bool {
			blocksCounter++
			statsOverall.takeStock(b)
			statsSection.takeStock(b)
			return yield(b)
		}

		// Helper to write section separator (empty block)
		sectionSeparator := func() bool {
			blocksCounter++
			return yield(indexBlock{}) // empty block as separator
		}

		// SECTION 1: Info Section
		statsInfo := startStats()
		info := src.Info()
		if !push(s.packInfoBlock(sealed.BlockInfo{Info: info}), &statsInfo) {
			return
		}

		// SECTION 2: Tokens Section
		statsTokens := startStats()
		allFieldsTables := []token.FieldTable{}
		tokensBlocks := bb.BuildTokenBlocks(src.TokenBlocks(consts.RegularBlockSize), src.Fields())
		for block, fieldsTables := range tokensBlocks {
			if !push(s.packTokenBlock(block), &statsTokens) {
				return
			}
			allFieldsTables = append(allFieldsTables, fieldsTables...)
		}
		if s.lastErr = util.CollapseErrors([]error{src.LastError(), bb.LastError()}); s.lastErr != nil {
			return
		}

		if !sectionSeparator() {
			return
		}

		// SECTION 3: Token Table Section
		statsTokenTable := startStats()
		tokenTableBlock := token.TableBlock{FieldsTables: collapseOrderedFieldsTables(allFieldsTables)}
		if !push(s.packTokenTableBlock(tokenTableBlock), &statsTokenTable) {
			return
		}

		if !sectionSeparator() {
			return
		}

		// SECTION 4: Offsets Section
		statsOffsets := startStats()
		offsets := sealed.BlockOffsets{
			IDsTotal: info.DocsTotal + 1, // +1 for system ID at position zero
			Offsets:  src.BlocksOffsets(),
		}
		if !push(s.packBlocksOffsetsBlock(offsets), &statsOffsets) {
			return
		}

		// SECTION 5: IDs Section
		s.idsTable.StartBlockIndex = blocksCounter // Record starting position for IDs blocks
		statsMIDs, statsRIDs, statsParams := startStats(), startStats(), startStats()
		for block := range createIDsSealBlocks(src.IDsBlocks(consts.IDsPerBlock)) {
			if !push(s.packMIDsBlock(block), &statsMIDs) {
				return
			}
			if !push(s.packRIDsBlock(block), &statsRIDs) {
				return
			}
			if !push(s.packPosBlock(block), &statsParams) {
				return
			}
		}
		if s.lastErr = src.LastError(); s.lastErr != nil {
			return
		}

		if !sectionSeparator() {
			return
		}

		// SECTION 6: LIDs Section
		statsLIDs := startStats()
		s.lidsTable.StartBlockIndex = blocksCounter
		for block := range bb.BuildLIDsBlocks(src.TokenLIDs(), consts.LIDBlockCap) {
			if !push(s.packLIDsBlock(block), &statsLIDs) {
				return
			}
		}
		if s.lastErr = util.CollapseErrors([]error{src.LastError(), bb.LastError()}); s.lastErr != nil {
			return
		}

		if !sectionSeparator() {
			return
		}

		// Log statistics for all sections
		endTime := time.Now()
		statsInfo.log("info", statsTokens.start)
		statsTokens.log("tokens", statsTokenTable.start)
		statsTokenTable.log("tokenTable", statsOffsets.start)
		statsOffsets.log("offsets", statsMIDs.start)
		statsMIDs.log("mids", statsLIDs.start)
		statsRIDs.log("rids", statsLIDs.start)
		statsParams.log("pos", statsLIDs.start)
		statsLIDs.log("lids", endTime)
		statsOverall.log("overall", endTime)
	}
}

// collapseOrderedFieldsTables merges field tables with identical field names
// Assumes the input array is already sorted by the Field property
func collapseOrderedFieldsTables(src []token.FieldTable) []token.FieldTable {
	if len(src) == 0 {
		return nil
	}
	current := src[0]
	dst := []token.FieldTable{}
	for _, ft := range src[1:] {
		if current.Field == ft.Field {
			current.Entries = append(current.Entries, ft.Entries...)
			continue
		}
		dst = append(dst, current)
		current = ft
	}
	dst = append(dst, current)
	return dst
}

// newIndexBlock creates an uncompressed index block.
func newIndexBlock(raw []byte) indexBlock {
	return indexBlock{
		codec:   storage.CodecNo,
		rawLen:  uint32(len(raw)),
		payload: raw,
	}
}

// newIndexBlockZSTD creates a compressed index block using ZSTD compression.
// Falls back to uncompressed if compression doesn't provide benefits.
func (s *IndexSealer) newIndexBlockZSTD(raw []byte, level int) indexBlock {
	s.buf2 = zstd.CompressLevel(raw, s.buf2[:0], level)
	// Only use compression if it actually reduces size
	if len(s.buf2) < len(raw) {
		return indexBlock{
			codec:   storage.CodecZSTD,
			rawLen:  uint32(len(raw)),
			payload: s.buf2,
		}
	}
	return newIndexBlock(raw)
}

// packInfoBlock packs fraction information into an index block.
func (s *IndexSealer) packInfoBlock(block sealed.BlockInfo) indexBlock {
	s.buf1 = block.Pack(s.buf1[:0])
	return newIndexBlock(s.buf1) // Info block is typically small, no compression
}

// packTokenBlock packs token data into a compressed index block.
func (s *IndexSealer) packTokenBlock(block tokensSealBlock) indexBlock {
	s.buf1 = block.payload.Pack(s.buf1[:0]) // Pack token data
	b := s.newIndexBlockZSTD(s.buf1, s.params.TokenListZstdLevel)
	// Store TID range in extended metadata
	b.ext1 = uint64(block.ext.maxTID)<<32 | uint64(block.ext.minTID)
	return b
}

// packTokenTableBlock packs the token table into a compressed index block.
func (s *IndexSealer) packTokenTableBlock(tokenTableBlock token.TableBlock) indexBlock {
	s.tokenTable = token.TableFromBlocks([]token.TableBlock{tokenTableBlock}) // Store for PreloadedData

	// Packing block
	s.buf1 = tokenTableBlock.Pack(s.buf1[:0])
	return s.newIndexBlockZSTD(s.buf1, s.params.TokenTableZstdLevel)
}

// packBlocksOffsetsBlock packs document block offsets into a compressed index block.
func (s *IndexSealer) packBlocksOffsetsBlock(block sealed.BlockOffsets) indexBlock {
	// Update IDs table for PreloadedData
	s.idsTable.IDsTotal = block.IDsTotal                  // Total number of IDs
	s.idsTable.IDBlocksTotal = uint32(len(block.Offsets)) // Number of ID blocks

	// Packing block
	s.buf1 = block.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.DocsPositionsZstdLevel)
	return b
}

// packMIDsBlock packs MIDs into a compressed index block.
func (s *IndexSealer) packMIDsBlock(block idsSealBlock) indexBlock {
	// Get the last ID in the block (smallest due to descending order)
	last := len(block.mids.Values) - 1
	minID := seq.ID{
		MID: seq.MID(block.mids.Values[last]),
		RID: seq.RID(block.rids.Values[last]),
	}
	s.idsTable.MinBlockIDs = append(s.idsTable.MinBlockIDs, minID) // Store for PreloadedData

	// Packing block
	s.buf1 = block.mids.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	// Store min MID and RID in extended metadata
	b.ext1 = uint64(minID.MID)
	b.ext2 = uint64(minID.RID)
	return b
}

// packRIDsBlock packs RIDs into a compressed index block.
func (s *IndexSealer) packRIDsBlock(block idsSealBlock) indexBlock {
	s.buf1 = block.rids.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	return b
}

// packPosBlock packs document positions into a compressed index block.
func (s *IndexSealer) packPosBlock(block idsSealBlock) indexBlock {
	s.buf1 = block.params.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	return b
}

// packLIDsBlock packs Local IDs (LIDs) into a compressed index block.
// Also updates LIDs table for preloaded data access.
func (s *IndexSealer) packLIDsBlock(block lidsSealBlock) indexBlock {
	var ext1 uint64
	if block.ext.isContinued { // todo: Legacy continuation flag
		ext1 = 1
		block.ext.minTID++ // Adjust for legacy format
	}

	// Update LIDs table for PreloadedData
	s.lidsTable.MinTIDs = append(s.lidsTable.MinTIDs, block.ext.minTID)
	s.lidsTable.MaxTIDs = append(s.lidsTable.MaxTIDs, block.ext.maxTID)
	s.lidsTable.IsContinued = append(s.lidsTable.IsContinued, block.ext.isContinued)

	// Packing block
	s.buf1 = block.payload.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.LIDsZstdLevel)
	b.ext1 = ext1                                                    // Legacy continuation flag
	b.ext2 = uint64(block.ext.maxTID)<<32 | uint64(block.ext.minTID) // TID range
	return b
}

// LIDsTable returns the built LIDs table for fast initialization of sealed fraction.
func (s *IndexSealer) LIDsTable() lids.Table {
	return s.lidsTable
}

// TokenTable returns the built token table for fast initialization of sealed fraction.
func (s *IndexSealer) TokenTable() token.Table {
	return s.tokenTable
}

// IDsTable returns the built IDs table for fast initialization of sealed fraction.
func (s *IndexSealer) IDsTable() seqids.Table {
	return s.idsTable
}
