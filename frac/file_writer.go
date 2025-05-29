package frac

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/ozontech/seq-db/metric/stopwatch"
)

type writeSyncer interface {
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
type FileWriter struct {
	ws       writeSyncer
	offset   atomic.Int64
	skipSync bool

	mu     sync.Mutex
	queue  []chan error
	notify chan struct{}

	wg sync.WaitGroup
}

func NewFileWriter(ws writeSyncer, offset int64, skipSync bool) *FileWriter {
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
	m := sw.Start("write_duration")

	dataLen := int64(len(data))
	offset := fs.offset.Add(dataLen) - dataLen
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
