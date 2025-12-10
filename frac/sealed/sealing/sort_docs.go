package sealing

import (
	"encoding/binary"
	"io"
	"iter"
	"os"
	"path/filepath"
	"time"

	"github.com/alecthomas/units"
	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
	"go.uber.org/zap"
)

type DocsSource interface {
	Docs() iter.Seq2[seq.ID, []byte]
	LastError() error
}

// SortDocs sorts documents and writes them in compressed form to disk.
// Creates a temporary file that is then renamed to the final one.
func SortDocs(name string, params frac.SealParams, ds DocsSource) ([]uint64, []seq.DocPos, int, error) {
	start := time.Now()
	logger.Info("sorting docs...")

	// Create temporary file for sorted documents
	sdocsFile, err := os.Create(name + consts.SdocsTmpFileSuffix)
	if err != nil {
		return nil, nil, 0, err
	}

	bw := bytespool.AcquireWriterSize(sdocsFile, int(units.MB))
	defer bytespool.ReleaseWriter(bw)

	// Group documents into blocks
	blocks := docBlocks(ds.Docs(), params.DocBlockSize)

	// Write blocks and get new offsets and positions
	blocksOffsets, positions, rawSize, onDiskSize, err := writeDocs(blocks, bw, params)

	if err := util.CollapseErrors([]error{ds.LastError(), err}); err != nil {
		return nil, nil, 0, err
	}
	if err := bw.Flush(); err != nil {
		return nil, nil, 0, err
	}

	// Synchronize and rename file
	if err := sdocsFile.Sync(); err != nil {
		return nil, nil, 0, err
	}
	if err := sdocsFile.Close(); err != nil {
		return nil, nil, 0, err
	}
	if err := os.Rename(sdocsFile.Name(), name+consts.SdocsFileSuffix); err != nil {
		return nil, nil, 0, err
	}
	if err := util.SyncPath(filepath.Dir(name)); err != nil {
		return nil, nil, 0, err
	}

	// Log compression statistics
	ratio := float64(rawSize) / float64(onDiskSize)
	logger.Info("docs sorting stat",
		util.ZapUint64AsSizeStr("raw", uint64(rawSize)),
		util.ZapUint64AsSizeStr("compressed", uint64(onDiskSize)),
		util.ZapFloat64WithPrec("ratio", ratio, 2),
		zap.Int("blocks_count", len(blocksOffsets)),
		zap.Int("docs_total", len(positions)),
		util.ZapDurationWithPrec("write_duration_ms", time.Since(start), "ms", 0),
	)

	return blocksOffsets, positions, onDiskSize, nil
}

// writeDocs compresses and writes document blocks, calculating new offsets
// and collecting document positions.
func writeDocs(
	blocks iter.Seq2[[]byte, []seq.DocPos],
	w io.Writer,
	params frac.SealParams,
) ([]uint64, []seq.DocPos, int, int, error) {
	offset := 0
	buf := make([]byte, 0)
	blocksOffsets := make([]uint64, 0)
	allPositions := make([]seq.DocPos, 0)

	rawSize := 0
	diskSize := 0

	// Process each document block
	for block, positions := range blocks {
		allPositions = append(allPositions, positions...)
		blocksOffsets = append(blocksOffsets, uint64(offset))

		// Compress document block
		buf = storage.CompressDocBlock(block, buf[:0], params.DocBlocksZstdLevel)

		rawSize += len(block)
		diskSize += len(buf)

		if _, err := w.Write(buf); err != nil {
			return nil, nil, 0, 0, err
		}
		offset += len(buf)
	}

	return blocksOffsets, allPositions, rawSize, diskSize, nil
}

// docBlocks groups documents into fixed-size blocks.
// Returns an iterator for blocks and corresponding document positions.
func docBlocks(docs iter.Seq2[seq.ID, []byte], blockSize int) iter.Seq2[[]byte, []seq.DocPos] {
	return func(yield func([]byte, []seq.DocPos) bool) {
		const defaultBlockSize = 128 * units.KiB
		if blockSize <= 0 {
			blockSize = int(defaultBlockSize)
			logger.Warn("document block size not specified", zap.Int("default_size", blockSize))
		}

		var (
			prev  seq.ID
			index uint32 // Current block index
		)
		pos := make([]seq.DocPos, 0)
		buf := make([]byte, 0, blockSize)

		// Iterate through documents
		for id, doc := range docs {
			if id == prev {
				// Duplicate IDs (for nested indexes) - store document once,
				// but create positions for each LID
				pos = append(pos, seq.PackDocPos(index, uint64(len(buf))))
				continue
			}
			prev = id

			// If block is full, yield it
			if len(buf) >= blockSize {
				if !yield(buf, pos) {
					return
				}
				index++
				buf = buf[:0]
				pos = pos[:0]
			}

			// Add document position
			pos = append(pos, seq.PackDocPos(index, uint64(len(buf))))

			// Write document size and the document itself
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(doc)))
			buf = append(buf, doc...)
		}
		yield(buf, pos)
	}
}
