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
	// WALMagic is the magic number at the start of WAL files
	WALMagic uint32 = 0xFFFFFFFF
	// WALVersion1 is the first version of WAL file with CRC32 checksums and 64 byte alignment for blocks.
	WALVersion1 uint8 = 1
	// WALCurrentVersion is the current WAL format version.
	WALCurrentVersion = WALVersion1
	// WALHeaderSize is the size of the WAL header in bytes (4 bytes magic + 1 byte version). 59 bytes are also reserved
	// due to alignment
	WALHeaderSize = 5
	// BlockAlignment is the alignment boundary for blocks in the new WAL format. Must be greater than
	// MetaBlock header (27 bytes) to prevent header torn writes and allow faster navigation during replay
	// of corrupted WAL file
	BlockAlignment int64 = 64
)

type WriteSyncer interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
}

// WalWriter writes MetaBlocks to a WAL file with header and 64-byte alignment.
// Format: [Header 5B] [... -> align to 64] [MetaBlock] [... -> align to 64] [MetaBlock] ...
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

	// write a header at the beggining if it's a new file
	if offset == 0 {
		if err := writeWALHeader(ws); err != nil {
			logger.Panic("failed to write WAL header", zap.Error(err))
		}

		if !skipSync {
			_ = ws.Sync()
		}
		w.offset.Store(WALHeaderSize)
	} else {
		w.offset.Store(nextBlockOffset(offset))
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

// Write writes a MetaBlock to the WAL file. The data must already be a MetaBlock.
// Returns the offset where the MetaBlock starts.
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
// is aligned to BlockAlignment
func (w *WalWriter) reserveSpace(blockSize int64) int64 {
	var result int64
	for {
		curr := w.offset.Load()
		nextSlotOffset := nextBlockOffset(curr)

		if w.offset.CompareAndSwap(curr, nextSlotOffset+blockSize) {
			result = nextSlotOffset
			break
		}
	}
	return result
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
	header := make([]byte, WALHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], WALMagic)
	header[4] = WALCurrentVersion
	_, err := w.WriteAt(header, 0)
	return err
}
