package frac

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/util"
)

var bulkSizeAfterCompression = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace: "seq_db_ingestor",
	Subsystem: "bulk",
	Name:      "bulk_size_after_compression",
	Help:      "Bulk request sizes after compression",
	Buckets:   prometheus.ExponentialBuckets(1024, 2, 16),
})

type DocsMetasCompressor struct {
	docsCompressLevel int
	metaCompressLevel int

	docsBuf disk.DocBlock
	metaBuf disk.DocBlock
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
	c.docsBuf = initBuf(c.docsBuf, len(docs))
	c.metaBuf = initBuf(c.metaBuf, len(meta))

	// Compress docs block.
	c.docsBuf = disk.CompressDocBlock(docs, c.docsBuf, c.docsCompressLevel)
	// Compress metas block.
	c.metaBuf = disk.CompressDocBlock(meta, c.metaBuf, c.metaCompressLevel)
	// Set compressed doc block size.
	c.metaBuf.SetExt1(uint64(len(c.docsBuf)))

	bulkSizeAfterCompression.Observe(float64(len(c.docsBuf) + len(c.metaBuf)))
}

func (c *DocsMetasCompressor) DocsMetas() ([]byte, []byte) {
	return c.docsBuf, c.metaBuf
}

func initBuf(buf []byte, size int) []byte {
	if buf == nil { // first usage when dst is not allocated
		const maxInitDocBlockSize = consts.MB
		return util.EnsureSliceSize(buf, min(maxInitDocBlockSize, size))
	}
	return buf
}
