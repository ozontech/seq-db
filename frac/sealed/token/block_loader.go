package token

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"unsafe"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/pattern"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

type Block struct {
	Payload     []byte
	Offsets     []uint32
	FreqIndexes []uint16 // indexes of tokens which have doc freqs (frequencies)
	Freqs       []uint32 // frequencies of certain tokens (how many docs have this token included at least once)
}

func (b *Block) Size() int {
	const selfSize = int(unsafe.Sizeof(Block{}))
	return selfSize +
		cap(b.Payload) +
		cap(b.Offsets)*util.SizeOfUint32 +
		cap(b.FreqIndexes)*util.SizeOfUint16 +
		cap(b.Freqs)*util.SizeOfUint32
}

func (b Block) Pack(dst []byte, buf []uint32) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(b.Payload)))
	dst = append(dst, b.Payload...)
	dst = packer.CompressDeltaBitpackUint16(dst, b.FreqIndexes, buf)
	return packer.CompressDeltaBitpackUint32(dst, b.Freqs, buf)
}

func (b *Block) Unpack(data []byte, fracVer config.BinaryDataVersion, unpackBuf *UnpackBuffer) error {
	if fracVer >= config.BinaryDataV5 {
		unpackBuf.Reset(fracVer)
		return b.unpackV5(data, unpackBuf)
	}
	return b.unpackV1(data)
}

func (b *Block) unpackV1(data []byte) error {
	b.Payload = append([]byte{}, data...)
	return b.parseTokenPayload(b.Payload)
}

func (b *Block) unpackV5(data []byte, buf *UnpackBuffer) error {
	if len(data) < util.SizeOfUint32 {
		return fmt.Errorf("token block too short: %d bytes", len(data))
	}

	payloadLen := binary.LittleEndian.Uint32(data[:util.SizeOfUint32])
	data = data[util.SizeOfUint32:]
	if uint32(len(data)) < payloadLen {
		return fmt.Errorf("invalid token block payload length: %d, data len %d", payloadLen, len(data))
	}

	payload := data[:payloadLen]
	data = data[payloadLen:]

	b.Payload = append(b.Payload[:0], payload...)

	if err := b.parseTokenPayload(payload); err != nil {
		return err
	}

	var err error
	var freqIndexes []uint16
	data, freqIndexes, err = packer.DecompressDeltaBitpackUint16(data, buf.decompressedUint16, buf.compressed)
	if err != nil {
		return err
	}
	b.FreqIndexes = append(b.FreqIndexes, freqIndexes...)

	var freqs []uint32
	_, freqs, err = packer.DecompressDeltaBitpackUint32(data, buf.decompressedUint32, buf.compressed)
	if err != nil {
		return err
	}
	b.Freqs = append(b.Freqs, freqs...)

	return nil
}

func (b *Block) parseTokenPayload(data []byte) error {
	b.Offsets = b.Offsets[:0]

	var offset uint32
	for i := 0; len(data) != 0; i++ {
		l := binary.LittleEndian.Uint32(data)
		data = data[util.SizeOfUint32:]
		offset += uint32(util.SizeOfUint32)
		if l == math.MaxUint32 {
			continue
		}
		if l > uint32(len(data)) {
			return fmt.Errorf("wrong field block for token %d, in pos %d", i, offset)
		}
		b.Offsets = append(b.Offsets, offset-uint32(util.SizeOfUint32))
		data = data[l:]
		offset += l
	}
	return nil
}

func (b *Block) Len() int {
	return len(b.Offsets)
}

// GetFreq returns frequency for a token if stored or 0 otherwise
func (b *Block) GetFreq(index int) uint32 {
	if b.Freqs == nil {
		return 0
	}

	idx := uint16(index)
	found := sort.Search(len(b.FreqIndexes), func(i int) bool { return b.FreqIndexes[i] >= idx })
	if found < len(b.FreqIndexes) && b.FreqIndexes[found] == idx {
		return b.Freqs[found]
	}
	return 0
}

func (b *Block) GetToken(index int) []byte {
	offset := b.Offsets[index]
	l := binary.LittleEndian.Uint32(b.Payload[offset:])
	offset += uint32(util.SizeOfUint32) // skip val length
	return b.Payload[offset : offset+l]
}

func (b *Block) contains(from, to int, needle []byte) ([]int, error) {
	indexes := make([]int, 0)
	for i := from; i <= to; i++ {
		if bytes.Contains(b.GetToken(i), needle) {
			indexes = append(indexes, i)
		}
	}
	return indexes, nil
}

func (b *Block) find(from, to int, searcher pattern.Searcher) ([]int, error) {
	indexes := make([]int, 0)
	for i := from; i <= to; i++ {
		ok, err := searcher.Check(b.GetToken(i))
		if err != nil {
			return nil, err
		}
		if ok {
			indexes = append(indexes, i)
		}
	}
	return indexes, nil
}

// BlockLoader is responsible for Reading from disk, unpacking and caching tokens blocks.
// NOT THREAD SAFE. Do not use concurrently.
// Use your own BlockLoader instance for each search query
type BlockLoader struct {
	fracName  string
	fracVer   config.BinaryDataVersion
	cache     *cache.Cache[*Block]
	reader    *storage.IndexReader
	unpackBuf *UnpackBuffer
	blockBuf  []byte
}

func NewBlockLoader(
	fracName string,
	fracVer config.BinaryDataVersion,
	reader *storage.IndexReader,
	c *cache.Cache[*Block],
) *BlockLoader {
	return &BlockLoader{
		fracName:  fracName,
		fracVer:   fracVer,
		cache:     c,
		reader:    reader,
		unpackBuf: &UnpackBuffer{},
	}
}

func (l *BlockLoader) Load(index uint32) *Block {
	block := l.cache.Get(index, func() (*Block, int) {
		block, err := l.read(index)
		if err != nil {
			logger.Panic("error reading tokens block", // todo: get rid of panic here
				zap.Error(err),
				zap.Uint32("index", index),
				zap.String("frac", l.fracName),
			)
		}
		size := block.Size()
		return block, size
	})
	return block
}

func (l *BlockLoader) read(index uint32) (*Block, error) {
	var err error
	l.blockBuf, _, err = l.reader.ReadIndexBlock(index, l.blockBuf)
	if err != nil {
		return nil, err
	}
	block := &Block{}
	err = block.Unpack(l.blockBuf, l.fracVer, l.unpackBuf)
	return block, err
}

type UnpackBuffer struct {
	decompressedUint32 []uint32 // temporary buffer for bitpack
	decompressedUint16 []uint16 // temporary buffer for bitpack
	compressed         []uint32 // temporary buffer for bitpack
}

func (b *UnpackBuffer) Reset(fracVer config.BinaryDataVersion) {
	if fracVer < config.BinaryDataV5 {
		return
	}
	if b.decompressedUint32 == nil {
		b.decompressedUint32 = make([]uint32, 0, 256)
	} else {
		b.decompressedUint32 = b.decompressedUint32[:0]
	}
	if b.compressed == nil {
		b.compressed = make([]uint32, 0, 256)
	} else {
		b.compressed = b.compressed[:0]
	}
}
