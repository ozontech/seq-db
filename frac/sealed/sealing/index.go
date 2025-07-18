package sealing

import (
	"bytes"
	"encoding/binary"
	"io"
	"iter"
	"time"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/frac/sealed"
	"github.com/ozontech/seq-db/frac/sealed/ids"
	"github.com/ozontech/seq-db/frac/sealed/lids"
	"github.com/ozontech/seq-db/frac/sealed/token"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
	"github.com/ozontech/seq-db/zstd"
)

type IndexSealer struct {
	lastErr error
	buf1    []byte // buffer for packing raw data
	buf2    []byte // buffer for compression
	params  common.SealParams

	// for PreloadedData
	idsTable   ids.Table
	lidsTable  lids.Table
	tokenTable token.Table
}

func NewIndexSealer(params common.SealParams) *IndexSealer {
	return &IndexSealer{
		params: params,
	}
}

type indexBlock struct {
	codec   disk.Codec
	payload []byte
	rawLen  int
	ext1    uint64
	ext2    uint64
}

func (i indexBlock) Bin(pos int64) (disk.IndexBlockHeader, []byte) {
	header := disk.NewIndexBlockHeader(pos, i.ext1, i.ext2, len(i.payload), i.rawLen, i.codec)
	return header, i.payload
}

func (s *IndexSealer) WriteIndex(ws io.WriteSeeker, src Source) error {
	const prefixSize = 16
	if _, err := ws.Seek(prefixSize, io.SeekStart); err != nil { // skip `prefixSize` bytes for pos and length of registry
		return err
	}

	hw := bytes.NewBuffer(nil)
	bw := bytespool.AcquireWriterSize(ws, consts.MB)
	defer bytespool.FlushReleaseWriter(bw)

	// write index blocks
	if err := s.writeBocks(prefixSize, bw, hw, src); err != nil {
		return err
	}
	bw.Flush()

	size := hw.Len()
	pos, err := ws.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// write registry
	bw.Write(hw.Bytes())
	bw.Flush()

	// write prefix
	prefix := make([]byte, 0, prefixSize)
	prefix = binary.LittleEndian.AppendUint64(prefix, uint64(pos))
	prefix = binary.LittleEndian.AppendUint64(prefix, uint64(size))
	if _, err := ws.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err = ws.Write(prefix); err != nil {
		return err
	}

	return nil
}

func (s *IndexSealer) writeBocks(pos int, payloadWriter, headersWriter io.Writer, src Source) error {
	for block := range s.indexBlocks(src) {
		header, payload := block.Bin(int64(pos))
		if _, err := payloadWriter.Write(payload); err != nil {
			return err
		}
		if _, err := headersWriter.Write(header); err != nil {
			return err
		}
		pos += len(payload)
	}
	if s.lastErr != nil {
		return s.lastErr
	}
	return nil
}

func (s *IndexSealer) indexBlocks(src Source) iter.Seq[indexBlock] {
	return func(yield func(indexBlock) bool) {
		bb := blocksBuilder{}
		blocsCounter := uint32(0)
		statsOverall := startStats()

		push := func(b indexBlock, stats *blocksStats) bool {
			blocsCounter++
			statsOverall.takeStock(b)
			if stats != nil {
				stats.takeStock(b)
			}
			return yield(b)
		}

		// -- Info --
		statsInfo := startStats()
		info := src.Info()
		if !push(s.packInfoBlock(sealed.BlockInfo{Info: info}), &statsInfo) {
			return
		}

		// -- Tokens Section --
		statsTokens := startStats()
		tokensBlocks, tokenTable := bb.TokensBlocksWithTokenTable(src.TokenBlocks(consts.RegularBlockSize), src.Fields())
		for block := range tokensBlocks {
			if !push(s.packTokenBlock(block), &statsTokens) {
				return
			}
		}
		if s.lastErr = util.CollapseErrors([]error{src.LastError(), bb.lastErr}); s.lastErr != nil {
			return
		}

		// -- Empty Separator --
		if !push(indexBlock{}, nil) {
			return
		}

		// -- Token Table --
		statsTokenTable := startStats()
		if !push(s.packTokenTableBlock(tokenTable()), &statsTokenTable) {
			return
		}

		// -- Empty Separator --
		if !push(indexBlock{}, nil) {
			return
		}

		// -- Offsets --
		statsOffsets := startStats()
		offsets := sealed.BlockOffsets{
			IDsTotal: info.DocsTotal + 1, // plus one ID - it is the system ID in zero position (see `frac.systemSeqID`)
			Offsets:  src.BlocksOffsets(),
		}
		if !push(s.packBlocksOffsetsBlock(offsets), &statsOffsets) {
			return
		}

		// -- IDs Section --
		s.idsTable.StartBlockIndex = blocsCounter
		statsMIDs, statsRIDs, statsParams := startStats(), startStats(), startStats()
		for block := range makeIDsSealBlocks(src.IDsBlocks(consts.IDsPerBlock)) {
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
		if s.lastErr = util.CollapseErrors([]error{src.LastError(), bb.lastErr}); s.lastErr != nil {
			return
		}

		// -- Empty Separator --
		if !push(indexBlock{}, nil) {
			return
		}

		// -- LIDs Section --
		statsLIDs := startStats()
		s.lidsTable.StartBlockIndex = blocsCounter
		for block := range bb.LIDsBlocks(src.TokenLIDs(), consts.LIDBlockCap) {
			if !push(s.packLIDsBlock(block), &statsLIDs) {
				return
			}
		}
		if s.lastErr = util.CollapseErrors([]error{src.LastError(), bb.lastErr}); s.lastErr != nil {
			return
		}

		// -- Empty Separator --
		if !push(indexBlock{}, nil) {
			return
		}

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

func (s *IndexSealer) indexBlockZSTD(raw []byte, level int) indexBlock {
	s.buf2 = zstd.CompressLevel(raw, s.buf2[:0], level)
	if len(s.buf2) < len(raw) {
		return indexBlock{
			codec:   disk.CodecZSTD,
			rawLen:  len(raw),
			payload: s.buf2,
		}
	}
	return indexBlock{
		codec:   disk.CodecNo,
		rawLen:  len(raw),
		payload: raw,
	}
}

func (s *IndexSealer) packInfoBlock(block sealed.BlockInfo) indexBlock {
	s.buf1 = block.Pack(s.buf1[:0])
	return indexBlock{
		codec:   disk.CodecNo,
		rawLen:  len(s.buf1),
		payload: s.buf1,
	}
}

func (s *IndexSealer) packTokenBlock(block tokensSealBlock) indexBlock {
	s.buf1 = block.payload.Pack(s.buf1[:0])
	b := s.indexBlockZSTD(s.buf1, s.params.TokenListZstdLevel)
	b.ext1 = uint64(block.ext.maxTID)<<32 | uint64(block.ext.minTID)
	return b
}

func (s *IndexSealer) packTokenTableBlock(tokenTable token.TableBlock) indexBlock {
	s.tokenTable = token.TableFromBlocks([]token.TableBlock{tokenTable}) // for PreloadedData

	s.buf1 = tokenTable.Pack(s.buf1[:0])
	return s.indexBlockZSTD(s.buf1, s.params.TokenTableZstdLevel)
}

func (s *IndexSealer) packBlocksOffsetsBlock(block sealed.BlockOffsets) indexBlock {
	s.idsTable.IDsTotal = block.IDsTotal                  // for PreloadedData
	s.idsTable.IDBlocksTotal = uint32(len(block.Offsets)) // for PreloadedData

	s.buf1 = block.Pack(s.buf1[:0])
	b := s.indexBlockZSTD(s.buf1, s.params.DocsPositionsZstdLevel)
	return b
}

func (s *IndexSealer) packMIDsBlock(block idsSealBlock) indexBlock {
	last := len(block.mids.Values) - 1
	minID := seq.ID{
		MID: seq.MID(block.mids.Values[last]),
		RID: seq.RID(block.rids.Values[last]),
	}
	s.idsTable.MinBlockIDs = append(s.idsTable.MinBlockIDs, minID) // for PreloadedData

	s.buf1 = block.mids.Pack(s.buf1[:0])
	b := s.indexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	b.ext1 = uint64(minID.MID)
	b.ext2 = uint64(minID.RID)
	return b
}

func (s *IndexSealer) packRIDsBlock(block idsSealBlock) indexBlock {
	s.buf1 = block.rids.Pack(s.buf1[:0])
	b := s.indexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	return b
}

func (s *IndexSealer) packPosBlock(block idsSealBlock) indexBlock {
	s.buf1 = block.params.Pack(s.buf1[:0])
	b := s.indexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	return b
}

func (s *IndexSealer) packLIDsBlock(block lidsSealBlock) indexBlock {
	var ext1 uint64
	if block.ext.isContinued { // todo: get rid of this confusion
		ext1 = 1
		block.ext.minTID++
	}

	// for PreloadedData
	s.lidsTable.MinTIDs = append(s.lidsTable.MinTIDs, block.ext.minTID)
	s.lidsTable.MaxTIDs = append(s.lidsTable.MaxTIDs, block.ext.maxTID)
	s.lidsTable.IsContinued = append(s.lidsTable.IsContinued, block.ext.isContinued)

	s.buf1 = block.payload.Pack(s.buf1[:0])
	b := s.indexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	b.ext1 = ext1
	b.ext2 = uint64(block.ext.maxTID)<<32 | uint64(block.ext.minTID)
	return b
}

func (s *IndexSealer) LIDsTable() *lids.Table {
	return &s.lidsTable
}
func (s *IndexSealer) TokenTable() token.Table {
	return s.tokenTable
}

func (s *IndexSealer) IDsTable() ids.Table {
	return s.idsTable
}
