package sealing

import (
	"bytes"
	"encoding/binary"
	"io"
	"iter"

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

// IndexSealer writes sealed fraction index data across multiple files.
// Each Write*File method writes one section to an independent file using the
// standard [prefix][blocks][registry] format so each file has its own IndexReader.
//
// Call order matters for PreloadedData:
//
//	WriteTokenAndLIDFiles → WriteOffsetsFile → WriteIDFile
//
// (TokenTable is populated by WriteTokenAndLIDFiles; IDsTable by WriteOffsetsFile+WriteIDFile;
// LIDsTable by WriteTokenAndLIDFiles.)
type IndexSealer struct {
	lastErr error
	buf1    []byte
	buf2    []byte
	params  common.SealParams

	idsTable   seqids.Table
	lidsTable  lids.Table
	tokenTable token.Table
}

func NewIndexSealer(params common.SealParams) *IndexSealer {
	return &IndexSealer{
		params: params,
		buf1:   make([]byte, 0, consts.RegularBlockSize),
		buf2:   make([]byte, 0, consts.RegularBlockSize),
	}
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

const filePrefixSize = 16

// write writes blocks to ws using [16-byte prefix][blocks][registry].
// The prefix is written last (via seek-back) and stores registry position + size.
func (s *IndexSealer) write(ws io.WriteSeeker, blocks iter.Seq[indexBlock]) error {
	if _, err := ws.Seek(filePrefixSize, io.SeekStart); err != nil {
		return err
	}

	hw := bytes.NewBuffer(nil)
	bw := bytespool.AcquireWriterSize(ws, int(units.MiB))
	defer bytespool.ReleaseWriter(bw)

	pos := filePrefixSize
	for block := range blocks {
		if s.lastErr != nil {
			return s.lastErr
		}
		header, payload := block.Bin(int64(pos))
		if _, err := bw.Write(payload); err != nil {
			return err
		}
		if _, err := hw.Write(header); err != nil {
			return err
		}
		pos += len(payload)
	}
	if s.lastErr != nil {
		return s.lastErr
	}
	if err := bw.Flush(); err != nil {
		return err
	}

	size := hw.Len()
	regPos, err := ws.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err := bw.Write(hw.Bytes()); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}

	prefix := binary.LittleEndian.AppendUint64(make([]byte, 0, filePrefixSize), uint64(regPos))
	prefix = binary.LittleEndian.AppendUint64(prefix, uint64(size))
	if _, err := ws.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err = ws.Write(prefix)
	return err
}

// fileStreamWriter writes blocks incrementally to a single file using the
// [prefix][blocks][registry] format, allowing interleaved writes to multiple files.
type fileStreamWriter struct {
	ws  io.WriteSeeker
	bw  *bytespool.Writer
	hw  bytes.Buffer
	pos int
}

func newFileStreamWriter(ws io.WriteSeeker) (*fileStreamWriter, error) {
	if _, err := ws.Seek(filePrefixSize, io.SeekStart); err != nil {
		return nil, err
	}
	return &fileStreamWriter{
		ws:  ws,
		bw:  bytespool.AcquireWriterSize(ws, int(units.MiB)),
		pos: filePrefixSize,
	}, nil
}

func (fw *fileStreamWriter) writeBlock(block indexBlock) error {
	header, payload := block.Bin(int64(fw.pos))
	if _, err := fw.bw.Write(payload); err != nil {
		return err
	}
	fw.hw.Write(header) // bytes.Buffer.Write never fails
	fw.pos += len(payload)
	return nil
}

func (fw *fileStreamWriter) finalize() (err error) {
	defer fw.release()
	if err = fw.bw.Flush(); err != nil {
		return
	}
	var regPos int64
	if regPos, err = fw.ws.Seek(0, io.SeekEnd); err != nil {
		return
	}
	if _, err = fw.bw.Write(fw.hw.Bytes()); err != nil {
		return
	}
	if err = fw.bw.Flush(); err != nil {
		return
	}
	prefix := binary.LittleEndian.AppendUint64(make([]byte, 0, filePrefixSize), uint64(regPos))
	prefix = binary.LittleEndian.AppendUint64(prefix, uint64(fw.hw.Len()))
	if _, err = fw.ws.Seek(0, io.SeekStart); err != nil {
		return
	}
	_, err = fw.ws.Write(prefix)
	return
}

func (fw *fileStreamWriter) release() {
	if fw.bw != nil {
		bytespool.ReleaseWriter(fw.bw)
		fw.bw = nil
	}
}

// WriteInfoFile writes the .info file containing a single BlockInfo block.
func (s *IndexSealer) WriteInfoFile(ws io.WriteSeeker, src Source) error {
	return s.write(ws, func(yield func(indexBlock) bool) {
		yield(s.packInfoBlock(sealed.BlockInfo{Info: src.Info()}))
	})
}

// WriteTokenAndLIDFiles writes the .token and .lid files in a single pass over the source data.
//
// .token file: [token blocks...] [separator] [token-table block] [separator]
// .lid file:   [LID blocks...] [separator]
//
// LID blocks are written interleaved with token block processing so that both files
// are produced from one sequential scan of the (token, LID) data.
func (s *IndexSealer) WriteTokenAndLIDFiles(tokenWS, lidWS io.WriteSeeker, src Source) error {
	tokenFW, err := newFileStreamWriter(tokenWS)
	if err != nil {
		return err
	}
	defer tokenFW.release()

	lidFW, err := newFileStreamWriter(lidWS)
	if err != nil {
		return err
	}
	defer lidFW.release()

	var (
		bb              blocksBuilder
		allFieldsTables []token.FieldTable
		lidAccum        = newLIDBlocksAccumulator(consts.LIDBlockCap)
	)

	accumulate := func(lids []uint32) error {
		return lidAccum.Add(lids, func(block lidsSealBlock) error {
			return lidFW.writeBlock(s.packLIDsBlock(block))
		})
	}

	blocks := bb.BuildTokenBlocks(
		src.Iterator(),
		accumulate, consts.RegularBlockSize,
	)

	for block, fieldsTables := range blocks {
		if err := tokenFW.writeBlock(s.packTokenBlock(block)); err != nil {
			return err
		}
		allFieldsTables = append(allFieldsTables, fieldsTables...)
	}

	if s.lastErr = util.CollapseErrors([]error{src.LastError(), bb.LastError()}); s.lastErr != nil {
		return s.lastErr
	}

	// Write the final (possibly partial) LID block and trailing separator.
	if err := lidFW.writeBlock(s.packLIDsBlock(lidAccum.Flush())); err != nil {
		return err
	}

	if err := lidFW.writeBlock(indexBlock{}); err != nil { // trailing separator
		return err
	}

	if err := lidFW.finalize(); err != nil {
		return err
	}

	// Write token section separator, token table, trailing separator.
	if err := tokenFW.writeBlock(indexBlock{}); err != nil { // section separator
		return err
	}
	tokenTableBlock := token.TableBlock{FieldsTables: collapseOrderedFieldsTables(allFieldsTables)}
	if err := tokenFW.writeBlock(s.packTokenTableBlock(tokenTableBlock)); err != nil {
		return err
	}
	if err := tokenFW.writeBlock(indexBlock{}); err != nil { // trailing separator
		return err
	}
	return tokenFW.finalize()
}

// WriteOffsetsFile writes the .offsets file containing a single BlockOffsets block.
func (s *IndexSealer) WriteOffsetsFile(ws io.WriteSeeker, src Source) error {
	return s.write(ws, func(yield func(indexBlock) bool) {
		offsets := sealed.BlockOffsets{
			IDsTotal: src.Info().DocsTotal + 1,
			Offsets:  src.BlockOffsets(),
		}
		yield(s.packBlocksOffsetsBlock(offsets))
	})
}

func (s *IndexSealer) WriteIDFile(ws io.WriteSeeker, src Source) error {
	return s.write(ws, func(yield func(indexBlock) bool) {
		for block := range seqBlockID(src.ID(), consts.IDsPerBlock) {
			if !yield(s.packMIDsBlock(block)) {
				return
			}

			if !yield(s.packRIDsBlock(block)) {
				return
			}

			if !yield(s.packPosBlock(block)) {
				return
			}
		}

		if s.lastErr = src.LastError(); s.lastErr != nil {
			return
		}

		yield(indexBlock{}) // trailing separator
	})
}

// collapseOrderedFieldsTables merges FieldTables with the same field name.
// Assumes input is sorted by Field.
func collapseOrderedFieldsTables(src []token.FieldTable) []token.FieldTable {
	if len(src) == 0 {
		return nil
	}

	current := src[0]
	var dst []token.FieldTable
	for _, ft := range src[1:] {
		if current.Field == ft.Field {
			current.Entries = append(current.Entries, ft.Entries...)
			continue
		}

		dst = append(dst, current)
		current = ft
	}

	return append(dst, current)
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

func (s *IndexSealer) packInfoBlock(block sealed.BlockInfo) indexBlock {
	s.buf1 = block.Pack(s.buf1[:0])
	return newIndexBlock(s.buf1)
}

func (s *IndexSealer) packTokenBlock(block tokensSealBlock) indexBlock {
	s.buf1 = block.payload.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.TokenListZstdLevel)
	b.ext1 = uint64(block.ext.maxTID)<<32 | uint64(block.ext.minTID)
	return b
}

func (s *IndexSealer) packTokenTableBlock(tb token.TableBlock) indexBlock {
	s.tokenTable = token.TableFromBlocks([]token.TableBlock{tb})
	s.buf1 = tb.Pack(s.buf1[:0])
	return s.newIndexBlockZSTD(s.buf1, s.params.TokenTableZstdLevel)
}

func (s *IndexSealer) packBlocksOffsetsBlock(block sealed.BlockOffsets) indexBlock {
	s.idsTable.IDsTotal = block.IDsTotal
	s.idsTable.IDBlocksTotal = uint32(len(block.Offsets))
	s.buf1 = block.Pack(s.buf1[:0])
	return s.newIndexBlockZSTD(s.buf1, s.params.DocsPositionsZstdLevel)
}

func (s *IndexSealer) packMIDsBlock(block idsSealBlock) indexBlock {
	last := len(block.mids.Values) - 1
	minID := seq.ID{MID: seq.MID(block.mids.Values[last]), RID: seq.RID(block.rids.Values[last])}
	s.idsTable.MinBlockIDs = append(s.idsTable.MinBlockIDs, minID)
	s.buf1 = block.mids.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
	b.ext1 = uint64(minID.MID)
	b.ext2 = uint64(minID.RID)
	return b
}

func (s *IndexSealer) packRIDsBlock(block idsSealBlock) indexBlock {
	s.buf1 = block.rids.Pack(s.buf1[:0])
	return s.newIndexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
}

func (s *IndexSealer) packPosBlock(block idsSealBlock) indexBlock {
	s.buf1 = block.params.Pack(s.buf1[:0])
	return s.newIndexBlockZSTD(s.buf1, s.params.IDsZstdLevel)
}

func (s *IndexSealer) packLIDsBlock(block lidsSealBlock) indexBlock {
	var ext1 uint64
	if block.ext.isContinued {
		ext1 = 1
		block.ext.minTID++
	}
	s.lidsTable.MinTIDs = append(s.lidsTable.MinTIDs, block.ext.minTID)
	s.lidsTable.MaxTIDs = append(s.lidsTable.MaxTIDs, block.ext.maxTID)
	s.lidsTable.IsContinued = append(s.lidsTable.IsContinued, block.ext.isContinued)
	s.buf1 = block.payload.Pack(s.buf1[:0])
	b := s.newIndexBlockZSTD(s.buf1, s.params.LIDsZstdLevel)
	b.ext1 = ext1
	b.ext2 = uint64(block.ext.maxTID)<<32 | uint64(block.ext.minTID)
	return b
}

func (s *IndexSealer) LIDsTable() lids.Table   { return s.lidsTable }
func (s *IndexSealer) TokenTable() token.Table { return s.tokenTable }
func (s *IndexSealer) IDsTable() seqids.Table  { return s.idsTable }
