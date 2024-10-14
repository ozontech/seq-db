// Package disk implements read write fraction routines
package disk

import (
	"time"

	"github.com/ozontech/seq-db/packer"
)

type BlockFormer struct {
	packer         *packer.BytesPacker
	writer         *BlocksWriter
	blockThreshold int

	// stats
	start time.Time
	stats BlockStats
}

type FlushOptions struct {
	ext1     uint64
	ext2     uint64
	compress bool
}

func NewDefaultFlushOptions() *FlushOptions {
	return &FlushOptions{
		ext1:     0,
		ext2:     0,
		compress: true,
	}
}

type FlushOption func(*FlushOptions)

func WithExt(ext1, ext2 uint64) FlushOption {
	return func(o *FlushOptions) {
		o.ext1 = ext1
		o.ext2 = ext2
	}
}

func WithCompress(compress bool) FlushOption {
	return func(o *FlushOptions) {
		o.compress = compress
	}
}

func NewBlockFormer(blockType string, writer *BlocksWriter, blockSize int, buf []byte) *BlockFormer {
	return &BlockFormer{
		packer:         packer.NewBytesPacker(buf[:0]),
		writer:         writer,
		blockThreshold: blockSize,
		start:          time.Now(),
		stats:          BlockStats{Name: blockType},
	}
}

func (b *BlockFormer) Packer() *packer.BytesPacker {
	return b.packer
}

func (b *BlockFormer) FlushIfNeeded(options ...FlushOption) (bool, error) {
	if len(b.packer.Data) > b.blockThreshold {
		return true, b.FlushForced(options...)
	}
	return false, nil
}

func (b *BlockFormer) FlushForced(options ...FlushOption) error {
	if len(b.packer.Data) == 0 {
		return nil
	}

	o := NewDefaultFlushOptions()
	for _, applyFn := range options {
		applyFn(o)
	}

	n, err := b.writer.WriteBlock(b.stats.Name, b.packer.Data, o.compress, o.ext1, o.ext2)
	if err != nil {
		return err
	}

	b.stats.Blocks++
	b.stats.Raw += uint64(len(b.packer.Data))
	b.stats.Comp += uint64(n)

	b.packer.Data = b.packer.Data[:0]
	return nil
}

func (b *BlockFormer) GetStats() *BlockStats {
	b.stats.Duration = time.Since(b.start)
	return &b.stats
}
