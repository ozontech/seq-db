package token

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
	"unsafe"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/packer"
	"github.com/ozontech/seq-db/util"
)

const sizeOfUint32 = uint32(unsafe.Sizeof(uint32(0)))

type StatsCollector interface {
	AddReadFieldTimeNS(time.Duration)
	AddDecodeFieldTimeNS(time.Duration)
	AddFieldBytesRead(uint64)
	AddFieldBlocksRead(uint64)
	AddValsLoaded(uint64)
}

type CacheEntry struct {
	Block, Offset []byte
}

func (t *CacheEntry) GetSize() int {
	const selfSize = int(unsafe.Sizeof(CacheEntry{}))
	return selfSize + cap(t.Block) + cap(t.Offset)
}

type Block struct {
	payload []byte
	offsets []byte
	entry   *TableEntry
}

func newBlock(entry *TableEntry, payload, offsets []byte) *Block {
	return &Block{
		entry:   entry,
		payload: payload,
		offsets: offsets,
	}
}

func (b *Block) GetValByTID(tid uint32) []byte {
	valIndex := b.entry.getIndexInTokensBlock(tid)
	offset := binary.LittleEndian.Uint32(b.offsets[valIndex*sizeOfUint32:])
	l := binary.LittleEndian.Uint32(b.payload[offset:])

	offset += sizeOfUint32 // skip val length
	return b.payload[offset : offset+l]
}

func (b *Block) unpack(data []byte) error {
	var offset uint32

	payload := data

	buf := bytespool.Acquire(4 * int(b.entry.ValCount))
	defer bytespool.Release(buf)

	offsetsPacker := packer.NewBytesPacker(buf.B[:0])

	for i := 0; len(data) != 0; i++ {
		l := binary.LittleEndian.Uint32(data)
		data = data[sizeOfUint32:]
		offset += sizeOfUint32

		if l == math.MaxUint32 {
			continue
		}
		if l > uint32(len(data)) {
			return fmt.Errorf("wrong field block for token %d, in pos %d", i, offset)
		}

		offsetsPacker.PutUint32(offset - sizeOfUint32)

		data = data[l:]
		offset += l
	}

	b.payload = payload
	b.offsets = append([]byte{}, offsetsPacker.Data...)

	return nil
}

func (b *Block) getCacheEntry() *CacheEntry {
	return &CacheEntry{
		Block:  b.payload,
		Offset: b.offsets,
	}
}

// BlockLoader is responsible for Reading from disk, unpacking and caching tokens blocks.
// NOT THREAD SAFE. Do not use concurrently.
// Use your own Loader instance for each search query
type BlockLoader struct {
	fracName string
	cache    *cache.Cache[*CacheEntry]
	reader   *disk.IndexReader
	stats    StatsCollector
}

func NewBlockLoader(
	fracName string,
	reader *disk.IndexReader,
	c *cache.Cache[*CacheEntry],
	stats StatsCollector,
) *BlockLoader {
	return &BlockLoader{
		fracName: fracName,
		cache:    c,
		reader:   reader,
		stats:    stats,
	}
}

func (l *BlockLoader) Load(entry *TableEntry) *Block {
	cacheEntry := l.cache.Get(entry.BlockIndex, func() (*CacheEntry, int) {
		tokenEntry := l.read(entry).getCacheEntry()
		size := tokenEntry.GetSize()
		return tokenEntry, size
	})
	return newBlock(entry, cacheEntry.Block, cacheEntry.Offset)
}

func (l *BlockLoader) readBinary(stats StatsCollector, blockIndex uint32) []byte {
	t := time.Now()
	data, n, err := l.reader.ReadIndexBlock(blockIndex, nil)
	if util.IsRecoveredPanicError(err) {
		logger.Panic("todo: handle read err", zap.Error(err))
	}

	if stats != nil {
		stats.AddReadFieldTimeNS(time.Since(t))
		stats.AddFieldBytesRead(n)
		stats.AddFieldBlocksRead(1)
	}

	return data
}

func (l *BlockLoader) read(entry *TableEntry) *Block {
	data := l.readBinary(l.stats, entry.BlockIndex)

	t := time.Now()
	var err error
	tokensBlock := newBlock(entry, nil, nil)
	if err = tokensBlock.unpack(data); err != nil {
		logger.Panic("error reading tokens block",
			zap.Error(err),
			zap.Any("entry", entry),
			zap.String("frac", l.fracName),
		)
	}

	if l.stats != nil {
		l.stats.AddDecodeFieldTimeNS(time.Since(t))
		l.stats.AddValsLoaded(uint64(entry.ValCount))
	}

	return tokensBlock
}
