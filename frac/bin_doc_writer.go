package frac

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"time"

	"github.com/alecthomas/units"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/common"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
	"github.com/ozontech/seq-db/util"
)

type binDocWriter struct {
	path      string
	file      *os.File
	bw        *bytespool.Writer
	start     time.Time
	rawBytes  uint64
	zstdLevel int

	prev       seq.ID
	blockIndex uint32
	blockSize  int
	blockBuf   []byte

	fileOffset    int
	compressBuf   []byte
	blocksOffsets []uint64

	positions map[seq.ID]seq.DocPos
}

func newBinDocWriter(path string, params common.SealParams) (*binDocWriter, error) {
	f, err := os.Create(path + consts.DocsTmpFileSuffix)
	if err != nil {
		return nil, err
	}

	return &binDocWriter{
		start: time.Now(),

		path: path,
		file: f,

		blockSize: params.DocBlockSize,
		blockBuf:  make([]byte, 0, params.DocBlockSize),

		zstdLevel: params.DocBlocksZstdLevel,
		bw:        bytespool.AcquireWriterSize(f, int(units.MiB)),

		positions: make(map[seq.ID]seq.DocPos),
	}, nil
}

func (w *binDocWriter) append(id seq.ID, doc []byte) error {
	if id == w.prev {
		return nil
	}

	w.prev = id
	if len(w.blockBuf) >= w.blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	w.positions[id] = seq.PackDocPos(w.blockIndex, uint64(len(w.blockBuf)))
	w.blockBuf = binary.LittleEndian.AppendUint32(w.blockBuf, uint32(len(doc)))
	w.blockBuf = append(w.blockBuf, doc...)

	w.rawBytes += uint64(len(doc))

	return nil
}

func (w *binDocWriter) flushBlock() error {
	w.blocksOffsets = append(w.blocksOffsets, uint64(w.fileOffset))

	w.compressBuf = storage.CompressDocBlock(w.blockBuf, w.compressBuf[:0], w.zstdLevel)
	n, err := w.bw.Write(w.compressBuf)
	if err != nil {
		return err
	}

	w.blockBuf = w.blockBuf[:0]
	w.fileOffset += n
	w.blockIndex++

	return nil
}

// flush writes the final block, syncs and renames the file.
// Returns block offsets and document positions needed to construct a BinnedSource.
func (w *binDocWriter) flush() (blocksOffsets []uint64, positions map[seq.ID]seq.DocPos, docsOnDisk uint64, err error) {
	defer bytespool.ReleaseWriter(w.bw)

	if err = w.flushBlock(); err != nil {
		return
	}

	if err = w.bw.Flush(); err != nil {
		return
	}

	stat, serr := w.file.Stat()
	if serr != nil {
		err = serr
		return
	}
	docsOnDisk = uint64(stat.Size())

	if err = w.file.Sync(); err != nil {
		return
	}

	if err = w.file.Close(); err != nil {
		return
	}

	if err = os.Rename(
		w.path+consts.DocsTmpFileSuffix,
		w.path+consts.DocsFileSuffix,
	); err != nil {
		return
	}

	if err = util.SyncPath(filepath.Dir(w.path)); err != nil {
		return
	}

	blocksOffsets = w.blocksOffsets
	positions = w.positions

	logger.Info("docs binning stats",
		util.ZapUint64AsSizeStr("raw", w.rawBytes),
		util.ZapUint64AsSizeStr("compressed", docsOnDisk),
		util.ZapFloat64WithPrec("ratio", float64(w.rawBytes)/float64(docsOnDisk), 2),
		zap.Int("blocks_count", len(blocksOffsets)),
		zap.Int("docs_total", len(positions)),
		util.ZapDurationWithPrec("write_duration_ms", time.Since(w.start), "ms", 0),
	)

	return
}
