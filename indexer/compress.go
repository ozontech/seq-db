package indexer

import (
	"sync"

	"github.com/alecthomas/units"

	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

type DocsMetasCompressor struct {
	docsCompressLevel int
	metaCompressLevel int

	docsBuf storage.DocBlock
	metaBuf storage.WalBlock
}

var compressorPool = sync.Pool{
	New: func() any {
		return &DocsMetasCompressor{}
	},
}

func GetDocsMetasCompressor(docsCompressLevel, metaCompressLevel int) *DocsMetasCompressor {
	compressor := compressorPool.Get().(*DocsMetasCompressor)
	compressor.docsCompressLevel = docsCompressLevel
	compressor.metaCompressLevel = metaCompressLevel
	return compressor
}

func PutDocMetasCompressor(c *DocsMetasCompressor) {
	compressorPool.Put(c)
}

// CompressDocsAndMetas prepare docs and meta blocks for bulk insert.
func (c *DocsMetasCompressor) CompressDocsAndMetas(docs, meta []byte) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() { // compress docs block
		c.docsBuf = initBuf(c.docsBuf, len(docs))
		c.docsBuf = storage.CompressDocBlock(docs, c.docsBuf, c.docsCompressLevel)
		wg.Done()
	}()

	go func() { // compress metas block
		c.metaBuf = initBuf(c.metaBuf, len(meta))
		c.metaBuf = storage.CompressWalBlock(meta, c.metaBuf, c.metaCompressLevel)
		wg.Done()
	}()

	wg.Wait()

	bulkSizeAfterCompression.Observe(float64(len(c.docsBuf) + len(c.metaBuf)))
}

func (c *DocsMetasCompressor) DocsMetas() ([]byte, []byte) {
	return c.docsBuf, c.metaBuf
}

func initBuf(buf []byte, size int) []byte {
	if buf == nil { // first usage when dst is not allocated
		const maxInitDocBlockSize = int(units.MiB)
		return util.EnsureSliceSize(buf, min(maxInitDocBlockSize, size))
	}
	return buf
}
