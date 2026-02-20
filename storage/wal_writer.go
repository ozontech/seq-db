package storage

import (
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric/stopwatch"
)

const (
	// WalMagic is the magic number at the start of WAL files
	WalMagic uint32 = 0xFFFFFFFF
	// WalVersion1 is the first version of WAL file with CRC32 checksums and 64 byte alignment for blocks.
	WalVersion1 uint8 = 1
	// WALCurrentVersion is the current WAL format version.
	WALCurrentVersion = WalVersion1
	// WalHeaderSize is the size of the WAL header in bytes (4 bytes magic + 1 byte version). 59 bytes are also reserved
	// due to alignment
	WalHeaderSize = 5
	// WalBlockAlignment is the alignment boundary for blocks in the new WAL format. Must be greater than
	// WalBlock header (27 bytes) to prevent header torn writes and allow faster navigation during replay
	// of corrupted WAL file
	WalBlockAlignment int64 = 64
)

type WriteSyncer interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
}

// WalWriter writes WalBlocks to a WAL file with header and 64-byte alignment.
// Format: [Header 5B] [... -> align to 64] [WalBlock] [... -> align to 64] [WalBlock] ...
type WalWriter struct {
	ws       WriteSyncer
	offset   atomic.Int64
	skipSync bool

	mu     sync.Mutex
	queue  []chan error
	notify chan struct{}

	wg sync.WaitGroup
}

func NewWalWriter(ws WriteSyncer, offset int64, skipSync bool) *WalWriter {
	w := &WalWriter{
		ws:       ws,
		skipSync: skipSync,
		notify:   make(chan struct{}, 1),
	}

	// write a header at the beginning if it's a new file
	if offset == 0 {
		if err := writeWALHeader(ws); err != nil {
			logger.Panic("failed to write WAL header", zap.Error(err))
		}

		if !skipSync {
			_ = ws.Sync()
		}

		w.offset.Store(alignSize(WalHeaderSize))
	}

	w.wg.Add(1)
	go func() {
		w.syncLoop()
		w.wg.Done()
	}()

	return w
}

func (w *WalWriter) syncLoop() {
	for range w.notify {
		w.mu.Lock()
		queue := w.queue
		w.queue = make([]chan error, 0, len(queue))
		w.mu.Unlock()

		err := w.ws.Sync()

		for _, syncRes := range queue {
			syncRes <- err
		}
	}
}

// Write writes a WalBlock to the WAL file. The data must already be a WalBlock.
// Returns the offset where the WalBlock starts.
func (w *WalWriter) Write(data []byte, sw *stopwatch.Stopwatch) (int64, error) {
	m := sw.Start("write_duration")

	offset := w.reserveSpace(int64(len(data)))

	if _, err := w.ws.WriteAt(data, offset); err != nil {
		m.Stop()
		return 0, err
	}
	m.Stop()

	err := w.sync(m, sw)

	return offset, err
}

// reserveSpace atomically reserves a necessary space and returns the next position where block may be written. The position
// is aligned to WalBlockAlignment
func (w *WalWriter) reserveSpace(blockSize int64) int64 {
	aligned := alignSize(blockSize)

	// w.offset is already aligned.
	// So when we add aligned block we still have aligned offset.
	end := w.offset.Add(aligned)
	start := end - aligned

	return start
}

func (w *WalWriter) sync(m stopwatch.Metric, sw *stopwatch.Stopwatch) error {
	if w.skipSync {
		return nil
	}

	m = sw.Start("fsync")

	syncRes := make(chan error)

	w.mu.Lock()
	w.queue = append(w.queue, syncRes)
	size := len(w.queue)
	w.mu.Unlock()

	if size == 1 {
		w.notify <- struct{}{}
	}

	err := <-syncRes

	m.Stop()
	return err
}

func (w *WalWriter) Stop() {
	close(w.notify)
	w.wg.Wait()
}

func writeWALHeader(w io.WriterAt) error {
	header := make([]byte, WalHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], WalMagic)
	header[4] = WALCurrentVersion
	_, err := w.WriteAt(header, 0)
	return err
}
