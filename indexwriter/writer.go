package indexwriter

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/alecthomas/units"
	"go.uber.org/zap"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/util"
)

const prefixSize = 16

const (
	blockTypeInfo   = "info"
	blockTypeOffset = "offset"

	blockTypeToken      = "token"
	blockTypeTokenTable = "token-table"

	blockTypeMID    = "mid"
	blockTypeRID    = "rid"
	blockTypeDocPos = "doc-pos"

	blockTypeLID = "lid"
)

// writer writes blocks incrementally to a single file using the
// [prefix][blocks][registry] format.
type writer struct {
	ws io.WriteSeeker

	wpayload *bytespool.Writer
	wheader  bytes.Buffer

	pos   int
	stats map[string]blockstat
}

type blockstat struct {
	count      int
	raw        int
	compressed int
	header     int
}

func (b blockstat) log(btype string) {
	logger.Info(
		"seal block stats",
		zap.String("type", btype),
		util.ZapUint64AsSizeStr("raw", uint64(b.raw)),
		util.ZapUint64AsSizeStr("compressed", uint64(b.compressed)),
		util.ZapUint64AsSizeStr("header", uint64(b.header)),
		zap.Uint64("blocks_count", uint64(b.count)),
	)
}

func newWriter(ws io.WriteSeeker) (*writer, error) {
	if _, err := ws.Seek(prefixSize, io.SeekStart); err != nil {
		return nil, err
	}

	return &writer{
		ws:       ws,
		wpayload: bytespool.AcquireWriterSize(ws, int(units.MiB)),
		pos:      prefixSize,
		stats:    make(map[string]blockstat),
	}, nil
}

func (w *writer) writeBlock(btype string, block indexBlock) error {
	header, payload := block.Bin(int64(w.pos))
	if _, err := w.wpayload.Write(payload); err != nil {
		return err
	}

	w.stats[btype] = blockstat{
		count:      w.stats[btype].count + 1,
		raw:        w.stats[btype].raw + int(block.rawLen),
		compressed: w.stats[btype].compressed + len(block.payload),
		header:     w.stats[btype].header + len(header),
	}

	w.wheader.Write(header)
	w.pos += len(payload)

	return nil
}

func (w *writer) writeEmptyBlock() error {
	header, _ := indexBlock{}.Bin(int64(w.pos))
	w.wheader.Write(header)
	return nil
}

func (w *writer) finalize() error {
	if err := w.wpayload.Flush(); err != nil {
		return err
	}

	regpos, err := w.ws.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if _, err := w.wpayload.Write(w.wheader.Bytes()); err != nil {
		return err
	}

	if err := w.wpayload.Flush(); err != nil {
		return err
	}

	prefix := make([]byte, 0, prefixSize)
	prefix = binary.LittleEndian.AppendUint64(prefix, uint64(regpos))
	prefix = binary.LittleEndian.AppendUint64(prefix, uint64(w.wheader.Len()))

	if _, err := w.ws.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if _, err := w.ws.Write(prefix); err != nil {
		return err
	}

	for btype, stats := range w.stats {
		stats.log(btype)
	}

	return nil
}

func (w *writer) release() {
	bytespool.ReleaseWriter(w.wpayload)
	w.wpayload = nil
}
