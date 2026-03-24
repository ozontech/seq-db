package storage

import (
	"encoding/binary"
	"io"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric/stopwatch"
)

const (
	// _WalVersionInitial is the first version of WAL file
	// with CRC32 checksums and 64 byte alignment for blocks.
	_WalVersionInitial byte = iota
)

const (
	// WalMagic is the magic number at the start of WAL files
	WalMagic uint32 = 0xFFFFFFFF
	// WalCurrentVersion is the current WAL format version.
	WalCurrentVersion = _WalVersionInitial
	// WalHeaderSize is the size of the WAL header in bytes (4 bytes magic + 1 byte version). 59 bytes are also reserved
	// due to alignment
	WalHeaderSize = 5
)

const (
	// WalBlockAlignment is the alignment boundary for blocks in the new WAL format. Must be greater than
	// WalBlock header (27 bytes) to prevent header torn writes and allow faster navigation during replay
	// of corrupted WAL file
	WalBlockAlignment int64 = 64
)

// WalWriter writes WalBlock to a WAL file with header and 64-byte alignment.
// It works on top of  FileWriter, but it also maintains header at the beginning of the file and align block
// offsets to WalBlockAlignment.
// Format: [Header 5B] [... -> align to 64] [WalBlock] [... -> align to 64] [WalBlock] ...
type WalWriter struct {
	fw *FileWriter
}

func NewWalWriter(ws fileWriterSyncer, offset int64, skipSync bool) *WalWriter {
	w := &WalWriter{}

	offset = align(offset)
	if offset == 0 {
		if err := writeWalHeader(ws); err != nil {
			logger.Panic("failed to write WAL header", zap.Error(err))
		}

		if !skipSync {
			_ = ws.Sync()
		}

		offset = align(WalHeaderSize)
	}

	w.fw = NewFileWriter(ws, offset, skipSync)
	return w
}

// Write writes a WalBlock to the WAL file. The data must already be a WalBlock.
// Returns the offset where the WalBlock starts.
func (w *WalWriter) Write(data WalBlock, sw *stopwatch.Stopwatch) (int64, error) {
	offset := w.fw.ReserveSpace(align(int64(len(data))))
	return w.fw.WriteAt(offset, data, sw)
}

func (w *WalWriter) Stop() {
	w.fw.Stop()
}

func writeWalHeader(w io.WriterAt) error {
	header := make([]byte, WalHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], WalMagic)
	header[4] = WalCurrentVersion
	_, err := w.WriteAt(header, 0)
	return err
}
