package indexwriter

import (
	"io"
	"iter"

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

type (
	DocLocation  = util.Pair[seq.ID, seq.DocPos]
	TokenPosting = util.Pair[[]byte, []uint32]
)

// Source defines the data required to write all index files for a fraction.
type Source interface {
	// Info returns metadata describing this source.
	Info() *common.Info

	// ID returns an iterator over stored document identifiers paired with
	// their positions, in descending [seq.ID] order.
	ID() iter.Seq2[DocLocation, error]

	// BlockOffsets returns byte offsets to each document block
	// within this source's `.docs` file.
	BlockOffsets() []uint64

	// TokenTriplet iterates over fields in lexicographic order.
	// For each field, it yields tokens (lexicographically sorted)
	// paired with the local document ID list for that token.
	TokenTriplet() iter.Seq2[string, iter.Seq2[TokenPosting, error]]
}

// indexBlock is one compressed (or not) block with its registry metadata.
type indexBlock struct {
	codec   storage.Codec
	payload []byte
	rawLen  uint32
	ext1    uint64
	ext2    uint64
}

func (i indexBlock) Bin(pos int64) (storage.IndexBlockHeader, []byte) {
	return storage.NewIndexBlockHeader(pos, i.ext1, i.ext2, uint32(len(i.payload)), i.rawLen, i.codec), i.payload
}

type IndexWriter struct {
	params common.SealParams

	buf1 []byte
	buf2 []byte

	idsTable   seqids.Table
	lidsTable  lids.Table
	tokenTable token.Table
}

func New(params common.SealParams) *IndexWriter {
	return &IndexWriter{
		params: params,
		buf1:   make([]byte, 0, consts.RegularBlockSize),
		buf2:   make([]byte, 0, consts.RegularBlockSize),
	}
}

func (s *IndexWriter) LIDsTable() lids.Table {
	return s.lidsTable
}

func (s *IndexWriter) TokenTable() token.Table {
	return s.tokenTable
}

func (s *IndexWriter) IDsTable() seqids.Table {
	return s.idsTable
}

// WriteOffsetsFile writes the .offsets file containing a single BlockOffsets block.
func (s *IndexWriter) WriteOffsetsFile(ws io.WriteSeeker, src Source) error {
	w, err := newWriter(ws)
	if err != nil {
		return err
	}
	defer w.release()

	offsets := sealed.BlockOffsets{Offsets: src.BlockOffsets()}
	if err := w.writeBlock(blockTypeOffset, s.packBlocksOffsetsBlock(offsets)); err != nil {
		return err
	}

	return w.finalize()
}

func (s *IndexWriter) WriteIDFile(ws io.WriteSeeker, src Source) error {
	w, err := newWriter(ws)
	if err != nil {
		return err
	}
	defer w.release()

	for block, err := range idBlock(src.ID(), consts.IDsPerBlock) {
		if err != nil {
			return err
		}

		if err := w.writeBlock(blockTypeMID, s.packMIDsBlock(block)); err != nil {
			return err
		}

		if err := w.writeBlock(blockTypeRID, s.packRIDsBlock(block)); err != nil {
			return err
		}

		if err := w.writeBlock(blockTypeDocPos, s.packPosBlock(block)); err != nil {
			return err
		}
	}

	return w.finalize()
}

func (s *IndexWriter) WriteTokenTriplet(tws, lws io.WriteSeeker, src Source) error {
	tw, err := newWriter(tws)
	if err != nil {
		return err
	}
	defer tw.release()

	lw, err := newWriter(lws)
	if err != nil {
		return err
	}
	defer lw.release()

	lidAccumulator := newLIDAccumulator(
		consts.LIDBlockCap,
		func(block unpackedLIDBlock) error {
			return lw.writeBlock(blockTypeLID, s.packLIDsBlock(block))
		},
	)

	var allFieldsTables []token.FieldTable
	for pair, err := range tokenBlock(src.TokenTriplet(), lidAccumulator.Add, consts.RegularBlockSize) {
		if err != nil {
			return err
		}

		if err := tw.writeBlock(blockTypeToken, s.packTokenBlock(pair.First)); err != nil {
			return err
		}

		allFieldsTables = append(allFieldsTables, pair.Second...)
	}

	if err := s.finalizeLIDFile(lw, lidAccumulator); err != nil {
		return err
	}

	return s.finalizeTokenFile(tw, allFieldsTables)
}

func (s *IndexWriter) finalizeLIDFile(w *writer, lidAccumulator *lidAccumulator) error {
	if err := lidAccumulator.Finalize(); err != nil {
		return err
	}

	return w.finalize()
}

func (s *IndexWriter) finalizeTokenFile(w *writer, allFieldsTables []token.FieldTable) error {
	// Emit section separator.
	if err := w.writeEmptyBlock(); err != nil {
		return err
	}

	tokenTableBlock := token.TableBlock{FieldsTables: collapseOrderedFieldsTables(allFieldsTables)}
	if err := w.writeBlock(blockTypeTokenTable, s.packTokenTableBlock(tokenTableBlock)); err != nil {
		return err
	}

	return w.finalize()
}

func (s *IndexWriter) WriteInfoFile(ws io.Writer, src Source) error {
	block := sealed.BlockInfo{Info: src.Info()}
	_, err := ws.Write(s.packInfoBlock(block).payload)
	return err
}

func newIndexBlock(raw []byte) indexBlock {
	return indexBlock{codec: storage.CodecNo, rawLen: uint32(len(raw)), payload: raw}
}

func (s *IndexWriter) newIndexBlockZSTD(raw []byte, level int) indexBlock {
	s.buf2 = zstd.CompressLevel(raw, s.buf2[:0], level)
	if len(s.buf2) < len(raw) {
		return indexBlock{codec: storage.CodecZSTD, rawLen: uint32(len(raw)), payload: s.buf2}
	}
	return newIndexBlock(raw)
}

// packInfoBlock packs fraction information into an index block.
func (s *IndexWriter) packInfoBlock(block sealed.BlockInfo) indexBlock {
	s.idsTable.IDsTotal = block.Info.DocsTotal + 1 // Increment by one for [seq.SystemID]
	s.buf1 = block.Pack(s.buf1[:0])
	return newIndexBlock(s.buf1) // Info block is typically small, no compression
}

// packTokenBlock packs token data into a compressed index block.
func (s *IndexWriter) packTokenBlock(block unpackedTokenBlock) indexBlock {
	s.buf1 = block.payload.Pack(s.buf1[:0]) // Pack token data
	b := s.newIndexBlockZSTD(s.buf1, s.params.TokenListZstdLevel)
	// Store TID range in extended metadata
	b.ext1 = uint64(block.ext.maxTID)<<32 | uint64(block.ext.minTID)
	return b
}

// packTokenTableBlock packs the token table into a compressed index block.
func (s *IndexWriter) packTokenTableBlock(tokenTableBlock token.TableBlock) indexBlock {
	s.tokenTable = token.TableFromBlocks([]token.TableBlock{tokenTableBlock}) // Store for PreloadedData

	// Packing block
	s.buf1 = tokenTableBlock.Pack(s.buf1[:0])
	return s.newIndexBlockZSTD(s.buf1, s.params.TokenTableZstdLevel)
}

// packBlocksOffsetsBlock packs document block offsets into a compressed index block.
func (s *IndexWriter) packBlocksOffsetsBlock(block sealed.BlockOffsets) indexBlock {
	// Packing block
	s.buf1 = block.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.DocsPositionsZstdLevel)
	return b
}

// packMIDsBlock packs MIDs into a compressed index block.
func (s *IndexWriter) packMIDsBlock(block unpackedIDBlock) indexBlock {
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
func (s *IndexWriter) packRIDsBlock(block unpackedIDBlock) indexBlock {
	s.buf1 = block.rids.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	return b
}

// packPosBlock packs document positions into a compressed index block.
func (s *IndexWriter) packPosBlock(block unpackedIDBlock) indexBlock {
	s.buf1 = block.params.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	return b
}

// packLIDsBlock packs Local IDs (LIDs) into a compressed index block.
// Also updates LIDs table for preloaded data access.
func (s *IndexWriter) packLIDsBlock(block unpackedLIDBlock) indexBlock {
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
