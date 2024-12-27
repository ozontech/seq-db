package disk

import (
	"io"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/zstd"
)

type BlocksWriter struct {
	writeSeeker    io.WriteSeeker
	curIndex       uint32
	blocksRegistry []byte
}

func NewBlocksWriter(ws io.WriteSeeker) *BlocksWriter {
	return &BlocksWriter{
		writeSeeker: ws,
	}
}

func (w *BlocksWriter) appendBlocksRegistry(entry IndexBlockHeader) {
	w.blocksRegistry = append(w.blocksRegistry, entry...)
	w.curIndex++
}

func (w *BlocksWriter) GetBlockIndex() uint32 {
	return w.curIndex
}

func (w *BlocksWriter) WriteEmptyBlock() {
	header := NewEmptyBlocksRegistryEntry()
	w.appendBlocksRegistry(header)
}

func (w *BlocksWriter) WriteBlock(blockType string, data []byte, compress bool, zstdLevel int, ext1, ext2 uint64) (uint32, error) {
	codec := CodecNo
	finalData := data
	if compress {
		codec = CodecZSTD
		compressed := bytespool.AcquireReset(len(data) + consts.RegularBlockSize)
		defer bytespool.Release(compressed)
		finalData = zstd.CompressLevel(data, compressed.B, zstdLevel)
		if len(finalData) >= len(data) {
			codec = CodecNo
			finalData = data
		}
	}

	pos, err := w.writeSeeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	w.appendBlocksRegistry(NewBlocksRegistryEntry(pos, ext1, ext2, data, finalData, codec))
	if _, err = w.writeSeeker.Write(finalData); err != nil {
		return 0, err
	}

	logger.Debug("write block",
		zap.String("block_type", blockType),
		zap.Int("raw", len(data)),
		zap.Int("written", len(finalData)),
		zap.Bool("compressed", compress),
		zap.Uint8("codec", uint8(codec)),
	)

	return uint32(len(finalData)), nil
}

func (w *BlocksWriter) WriteBlocksRegistry() error {
	pos, err := w.writeSeeker.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	size, err := w.writeSeeker.Write(w.blocksRegistry)
	if err != nil {
		return err
	}

	p := packer.NewBytesPacker(make([]byte, 0, 16))
	p.PutUint64(uint64(pos))
	p.PutUint64(uint64(size))

	if _, err = w.writeSeeker.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if _, err = w.writeSeeker.Write(p.Data); err != nil {
		return err
	}

	return nil
}
