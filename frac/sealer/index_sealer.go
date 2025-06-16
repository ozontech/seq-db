package sealer

import (
	"bytes"
	"encoding/binary"
	"io"
	"iter"
	"time"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/frac/ids"
	"github.com/ozontech/seq-db/frac/lids"
	"github.com/ozontech/seq-db/frac/token"
	"github.com/ozontech/seq-db/zstd"
)

type IndexSealer struct {
	err    error
	buf1   []byte // buffer for packing raw data
	buf2   []byte // buffer for compression
	params frac.SealParams
	stats  stats

	// for PreloadedData
	lidsTable  lids.Table
	tokenTable token.Table
	idsTable   ids.Table
}

func NewIndexSealer(params frac.SealParams) *IndexSealer {
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
	if s.err != nil {
		return s.err
	}
	return nil
}

func (s *IndexSealer) indexBlocks(src Source) iter.Seq[indexBlock] {
	return func(yield func(indexBlock) bool) {
		blockNum := uint32(1)
		bb := BlocksBuilder{}

		// -- Info --
		s.stats.info = startStats()
		info := src.Info()
		if !yield(s.stats.info.account(s.packInfoBlock(frac.BlockInfo{Info: info}))) {
			return
		}
		blockNum++

		// -- Tokens Section --
		s.stats.tokens = startStats()
		tokensBlocks, tokenTable := bb.TokensBlocksWithTokenTable(src.Tokens(), src.Fields(), consts.RegularBlockSize)
		for block := range tokensBlocks {
			if !yield(s.stats.tokens.account(s.packTokenBlock(block))) {
				return
			}
			blockNum++
		}
		if s.err = bb.err; s.err != nil {
			return
		}

		// -- Empty Separator --
		if !yield(indexBlock{}) {
			return
		}
		blockNum++

		// -- Token Table --
		s.stats.tokenTable = startStats()
		if !yield(s.stats.tokenTable.account(s.packTokenTableBlock(tokenTable()))) {
			return
		}
		blockNum++

		// -- Empty Separator --
		if !yield(indexBlock{}) {
			return
		}
		blockNum++

		// -- Offsets --
		s.stats.offsets = startStats()
		offsets := frac.BlockBlocksOffsets{
			IDsTotal:      info.DocsTotal, // todo +1? nested?
			BlocksOffsets: src.BlocksOffsets(),
		}
		if !yield(s.stats.offsets.account(s.packBlocksOffsetsBlock(offsets))) {
			return
		}
		blockNum++

		// -- Empty Separator --
		if !yield(indexBlock{}) {
			return
		}
		blockNum++

		// -- IDs Section --
		s.idsTable.StartBlockIndex = blockNum
		s.stats.mids, s.stats.rids, s.stats.pos = startStats(), startStats(), startStats()
		for block := range bb.IDsBlocks(src.IDs(), src.Pos(), consts.IDsBlockSize) {
			if !yield(s.stats.mids.account(s.packMIDsBlock(block))) {
				return
			}
			if !yield(s.stats.rids.account(s.packRIDsBlock(block))) {
				return
			}
			if !yield(s.stats.pos.account(s.packPosBlock(block))) {
				return
			}
			blockNum += 3
		}
		if s.err = bb.err; s.err != nil {
			return
		}

		// -- Empty Separator --
		if !yield(indexBlock{}) {
			return
		}
		blockNum++

		// -- LIDs Section --
		s.stats.lids = startStats()
		s.lidsTable.StartBlockIndex = blockNum
		isContinued := false
		for block := range bb.LIDsBlocks(src.TokenLIDs(), consts.LIDBlockCap) {
			block.IsContinued = isContinued // todo: get rid of IsContinued
			if !yield(s.stats.lids.account(s.packLIDsBlock(block))) {
				return
			}
			blockNum++
			isContinued = block.Chunks.IsLastLID
		}
		if s.err = bb.err; s.err != nil {
			return
		}

		logStats(s.stats, time.Now())
	}
}

func logStats(s stats, endTime time.Time) {
	s.info.log("info", s.tokens.start)
	s.info.log("tokens", s.tokenTable.start)
	s.tokenTable.log("tokenTable", s.offsets.start)
	s.offsets.log("offsets", s.mids.start)
	s.mids.log("mids", s.lids.start)
	s.rids.log("rids", s.lids.start)
	s.pos.log("pos", s.lids.start)
	s.lids.log("lids", endTime)
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

func (s *IndexSealer) packInfoBlock(block frac.BlockInfo) indexBlock {
	s.buf1 = block.Pack(s.buf1[:0])
	return indexBlock{
		codec:   disk.CodecNo,
		rawLen:  len(s.buf1),
		payload: s.buf1,
	}
}

func (s *IndexSealer) packTokenBlock(block token.Block) indexBlock {
	b := s.indexBlockZSTD(block.Pack(), s.params.TokenListZstdLevel)
	b.ext1 = uint64(block.MaxTID)<<32 | uint64(block.MinTID)
	return b
}

func (s *IndexSealer) packTokenTableBlock(tokenTable token.TableBlock) indexBlock {
	s.tokenTable = make(token.Table)
	tokenTable.FillTable(s.tokenTable) // for PreloadedData

	s.buf1 = tokenTable.Pack(s.buf1[:0])
	return s.indexBlockZSTD(s.buf1, s.params.TokenTableZstdLevel)
}

func (s *IndexSealer) packBlocksOffsetsBlock(block frac.BlockBlocksOffsets) indexBlock {
	s.idsTable.IDsTotal = block.IDsTotal                        // for PreloadedData
	s.idsTable.IDBlocksTotal = uint32(len(block.BlocksOffsets)) // for PreloadedData

	s.buf1 = block.Pack(s.buf1[:0])
	b := s.indexBlockZSTD(s.buf1, s.params.DocsPositionsZstdLevel)
	return b
}

func (s *IndexSealer) packMIDsBlock(block ids.Block) indexBlock {
	minID := block.MinID()
	s.idsTable.MinBlockIDs = append(s.idsTable.MinBlockIDs, minID) // for PreloadedData

	s.buf1 = block.PackMIDs(s.buf1[:0])
	b := s.indexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	b.ext1 = uint64(minID.MID)
	b.ext2 = uint64(minID.RID)
	return b
}

func (s *IndexSealer) packRIDsBlock(block ids.Block) indexBlock {
	s.buf1 = block.PackRIDs(s.buf1[:0])
	b := s.indexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	return b
}

func (s *IndexSealer) packPosBlock(block ids.Block) indexBlock {
	s.buf1 = block.PackPos(s.buf1[:0])
	b := s.indexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	return b
}

func (s *IndexSealer) packLIDsBlock(block lids.Block) indexBlock {
	var ext1 uint64
	if block.IsContinued { // todo: get rid of this confusion
		ext1 = 1
		block.MinTID++
	}

	s.lidsTable.Add(&block) // for PreloadedData

	s.buf1 = block.Chunks.Pack(s.buf1[:0])
	b := s.indexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	b.ext1 = ext1
	b.ext2 = uint64(block.MaxTID)<<32 | uint64(block.MinTID)
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
