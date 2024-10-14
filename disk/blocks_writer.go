package disk

import (
	"io"

	"github.com/pierrec/lz4/v4"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/packer"
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

func (w *BlocksWriter) appendBlocksRegistry(entry BlocksRegistryEntry) {
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

func (w *BlocksWriter) WriteBlock(blockType string, data []byte, compress bool, ext1, ext2 uint64) (uint32, error) {
	codec := CodecLZ4

	var err error
	var finalData []byte

	if compress {
		compressed := bytespool.Acquire(len(data) + consts.RegularBlockSize)
		defer bytespool.Release(compressed)

		n, err := lz4.CompressBlock(data, compressed.B, nil)
		if err != nil {
			return 0, err
		}
		finalData = compressed.B[:n]
	}

	if len(finalData) == 0 {
		finalData = data
		codec = CodecNo
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
