package storage

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/ozontech/seq-db/metric/stopwatch"
)

type fileWriterSyncer interface {
	io.WriterAt
	Sync() error
}

// FileWriter optimizes sequential writing and fsync calls for concurrent writers.
//
// The write offset is calculated strictly sequentially using atomic. After that, a request for fsync is sent.
// The request waits if fsync is being performed from previous requests. During this wait, other fsync
// requests may arrive that are also waiting for the previous one to complete. After that, a new fsync
// is performed, after which all requests receive a response about the successful (or unsuccessful) fsync.
//
// This results in one fsync system call for several writers performing a write at approximately the same time.
//
// FileWriter does not interpret block format; it only writes bytes and triggers fsync.
type FileWriter struct {
	ws       fileWriterSyncer
	offset   atomic.Int64
	skipSync bool

	mu     sync.Mutex
	queue  []chan error
	notify chan struct{}

	wg sync.WaitGroup
}

// NewFileWriter creates a new FileWriter. offset is the initial write position for sequential Write.
func NewFileWriter(ws fileWriterSyncer, offset int64, skipSync bool) *FileWriter {
	fs := &FileWriter{
		ws:       ws,
		skipSync: skipSync,
		notify:   make(chan struct{}, 1),
	}

	fs.offset.Store(offset)

	fs.wg.Add(1)
	go func() {
		fs.syncLoop()
		fs.wg.Done()
	}()

	return fs
}

func (fs *FileWriter) syncLoop() {
	for range fs.notify {
		fs.mu.Lock()
		queue := fs.queue
		fs.queue = make([]chan error, 0, len(queue))
		fs.mu.Unlock()

		err := fs.ws.Sync()

		for _, syncRes := range queue {
			syncRes <- err
		}
	}
}

func (fs *FileWriter) Write(data []byte, sw *stopwatch.Stopwatch) (int64, error) {
	offset := fs.ReserveSpace(int64(len(data)))
	return fs.writeAt(offset, data, sw)
}

func (fs *FileWriter) WriteAt(offset int64, data []byte, sw *stopwatch.Stopwatch) (int64, error) {
	return fs.writeAt(offset, data, sw)
}
func (fs *FileWriter) ReserveSpace(size int64) int64 {
	end := fs.offset.Add(size)
	start := end - size
	return start
}

func (fs *FileWriter) writeAt(offset int64, data []byte, sw *stopwatch.Stopwatch) (int64, error) {
	m := sw.Start("write_duration")

	_, err := fs.ws.WriteAt(data, offset)
	m.Stop()

	if err != nil {
		return 0, err
	}

	if fs.skipSync {
		return offset, nil
	}

	m = sw.Start("fsync")

	syncRes := make(chan error)

	fs.mu.Lock()
	fs.queue = append(fs.queue, syncRes)
	size := len(fs.queue)
	fs.mu.Unlock()

	if size == 1 {
		fs.notify <- struct{}{}
	}

	err = <-syncRes

	m.Stop()

	return offset, err
}

func (fs *FileWriter) Stop() {
	close(fs.notify)
	fs.wg.Wait()
}
