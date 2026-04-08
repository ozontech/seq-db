package sealing

import (
	"io"

	"github.com/ozontech/seq-db/blockbuilder"
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

type IndexSealer struct {
	params common.SealParams

	buf1 []byte
	buf2 []byte

	idsTable   seqids.Table
	lidsTable  lids.Table
	tokenTable token.Table

	lastErr error
}

func NewIndexSealer(params common.SealParams) *IndexSealer {
	return &IndexSealer{
		params: params,
		buf1:   make([]byte, 0, consts.RegularBlockSize),
		buf2:   make([]byte, 0, consts.RegularBlockSize),
	}
}

func (s *IndexSealer) LIDsTable() lids.Table {
	return s.lidsTable
}

func (s *IndexSealer) TokenTable() token.Table {
	return s.tokenTable
}

func (s *IndexSealer) IDsTable() seqids.Table {
	return s.idsTable
}

// WriteOffsetsFile writes the .offsets file containing a single BlockOffsets block.
func (s *IndexSealer) WriteOffsetsFile(ws io.WriteSeeker, src Source) error {
	w, err := newWriter(ws)
	if err != nil {
		return err
	}
	defer w.release()

	offsets := sealed.BlockOffsets{
		IDsTotal: src.Info().DocsTotal + 1,
		Offsets:  src.BlockOffsets(),
	}

	if err := w.writeBlock(btypeOffset, s.packBlocksOffsetsBlock(offsets)); err != nil {
		return err
	}

	// Emit trailing separator.
	if err := w.writeBlock(btypeBlackhole, indexBlock{}); err != nil {
		return err
	}

	return w.finalize()
}

func (s *IndexSealer) WriteIDFile(ws io.WriteSeeker, src Source) error {
	w, err := newWriter(ws)
	if err != nil {
		return err
	}
	defer w.release()

	for block := range blockbuilder.SeqBlockID(src.ID(), consts.IDsPerBlock) {
		if err := w.writeBlock(btypeMid, s.packMIDsBlock(block)); err != nil {
			return err
		}

		if err := w.writeBlock(btypeRid, s.packRIDsBlock(block)); err != nil {
			return err
		}

		if err := w.writeBlock(btypeDocPos, s.packPosBlock(block)); err != nil {
			return err
		}
	}

	// Emit trailing separator.
	if err := w.writeBlock(btypeBlackhole, indexBlock{}); err != nil {
		return err
	}

	return w.finalize()
}

func (s *IndexSealer) WriteTokenTriplet(tws, lws io.WriteSeeker, src Source) error {
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

	var (
		bb              blockbuilder.BlocksBuilder
		allFieldsTables []token.FieldTable
		lidacc          = blockbuilder.NewLIDBlocksAccumulator(consts.LIDBlockCap)
	)

	// NOTE(dkharms): This is so ugly but I cannot come up with other solution here.
	accumulate := func(lids []uint32) error {
		return lidacc.Add(lids, func(block blockbuilder.LidsSealBlock) error {
			return lw.writeBlock(btypeLid, s.packLIDsBlock(block))
		})
	}

	for block, fieldsTables := range bb.BuildTokenBlocks(src.TokenTriplet(), accumulate, consts.RegularBlockSize) {
		if err := tw.writeBlock(btypeToken, s.packTokenBlock(block)); err != nil {
			return err
		}
		allFieldsTables = append(allFieldsTables, fieldsTables...)
	}

	if s.lastErr = util.CollapseErrors([]error{src.LastError(), bb.LastError()}); s.lastErr != nil {
		return s.lastErr
	}

	if err := s.finalizeLIDFile(lw, lidacc); err != nil {
		return err
	}

	return s.finalizeTokenFile(tw, allFieldsTables)
}

func (s *IndexSealer) finalizeLIDFile(w *writer, lidAccum *blockbuilder.LidBlocksAcc) error {
	if err := w.writeBlock(btypeLid, s.packLIDsBlock(lidAccum.Flush())); err != nil {
		return err
	}

	// Emit trailing separator.
	if err := w.writeBlock(btypeBlackhole, indexBlock{}); err != nil {
		return err
	}

	return w.finalize()
}

func (s *IndexSealer) finalizeTokenFile(w *writer, allFieldsTables []token.FieldTable) error {
	// Emit section separator.
	if err := w.writeBlock(btypeToken, indexBlock{}); err != nil {
		return err
	}

	tokenTableBlock := token.TableBlock{FieldsTables: blockbuilder.CollapseOrderedFieldsTables(allFieldsTables)}
	if err := w.writeBlock(btypeTokenTable, s.packTokenTableBlock(tokenTableBlock)); err != nil {
		return err
	}

	// Emit trailing separator.
	if err := w.writeBlock(btypeBlackhole, indexBlock{}); err != nil {
		return err
	}

	return w.finalize()
}

func (s *IndexSealer) WriteInfoFile(ws io.WriteSeeker, src Source) error {
	w, err := newWriter(ws)
	if err != nil {
		return err
	}
	defer w.release()

	block := sealed.BlockInfo{Info: src.Info()}
	if err := w.writeBlock(btypeInfo, s.packInfoBlock(block)); err != nil {
		return err
	}

	// Emit trailing separator.
	if err := w.writeBlock(btypeBlackhole, indexBlock{}); err != nil {
		return err
	}

	return w.finalize()
}

func newIndexBlock(raw []byte) indexBlock {
	return indexBlock{codec: storage.CodecNo, rawLen: uint32(len(raw)), payload: raw}
}

func (s *IndexSealer) newIndexBlockZSTD(raw []byte, level int) indexBlock {
	s.buf2 = zstd.CompressLevel(raw, s.buf2[:0], level)
	if len(s.buf2) < len(raw) {
		return indexBlock{codec: storage.CodecZSTD, rawLen: uint32(len(raw)), payload: s.buf2}
	}
	return newIndexBlock(raw)
}

// packInfoBlock packs fraction information into an index block.
func (s *IndexSealer) packInfoBlock(block sealed.BlockInfo) indexBlock {
	s.buf1 = block.Pack(s.buf1[:0])
	return newIndexBlock(s.buf1) // Info block is typically small, no compression
}

// packTokenBlock packs token data into a compressed index block.
func (s *IndexSealer) packTokenBlock(block blockbuilder.TokensSealBlock) indexBlock {
	s.buf1 = block.Payload.Pack(s.buf1[:0]) // Pack token data
	b := s.newIndexBlockZSTD(s.buf1, s.params.TokenListZstdLevel)
	// Store TID range in extended metadata
	b.ext1 = uint64(block.Ext.MaxTID)<<32 | uint64(block.Ext.MinTID)
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
func (s *IndexSealer) packMIDsBlock(block blockbuilder.IdsSealBlock) indexBlock {
	// Get the last ID in the block (smallest due to descending order)
	last := len(block.MIDs.Values) - 1

	minID := seq.ID{
		MID: seq.MID(block.MIDs.Values[last]),
		RID: seq.RID(block.RIDs.Values[last]),
	}

	s.idsTable.MinBlockIDs = append(s.idsTable.MinBlockIDs, minID) // Store for PreloadedData

	// Packing block
	s.buf1 = block.MIDs.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.IDsZstdLevel)

	// Store min MID and RID in extended metadata
	b.ext1 = uint64(minID.MID)
	b.ext2 = uint64(minID.RID)

	return b
}

// packRIDsBlock packs RIDs into a compressed index block.
func (s *IndexSealer) packRIDsBlock(block blockbuilder.IdsSealBlock) indexBlock {
	s.buf1 = block.RIDs.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	return b
}

// packPosBlock packs document positions into a compressed index block.
func (s *IndexSealer) packPosBlock(block blockbuilder.IdsSealBlock) indexBlock {
	s.buf1 = block.Params.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	return b
}

// packLIDsBlock packs Local IDs (LIDs) into a compressed index block.
// Also updates LIDs table for preloaded data access.
func (s *IndexSealer) packLIDsBlock(block blockbuilder.LidsSealBlock) indexBlock {
	var ext1 uint64
	if block.Ext.IsContinued { // todo: Legacy continuation flag
		ext1 = 1
		block.Ext.MinTID++ // Adjust for legacy format
	}

	// Update LIDs table for PreloadedData
	s.lidsTable.MinTIDs = append(s.lidsTable.MinTIDs, block.Ext.MinTID)
	s.lidsTable.MaxTIDs = append(s.lidsTable.MaxTIDs, block.Ext.MaxTID)
	s.lidsTable.IsContinued = append(s.lidsTable.IsContinued, block.Ext.IsContinued)

	// Packing block
	s.buf1 = block.Payload.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.LIDsZstdLevel)
	b.ext1 = ext1                                                    // Legacy continuation flag
	b.ext2 = uint64(block.Ext.MaxTID)<<32 | uint64(block.Ext.MinTID) // TID range

	return b
}
