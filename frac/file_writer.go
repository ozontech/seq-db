package frac

import (
	"io"
	"sync"

	"github.com/ozontech/seq-db/metric/stopwatch"
)

type writeSyncer interface {
	io.Writer
	Sync() error
}

type FileWriter struct {
	ws writeSyncer

	skipSync bool

	mu     sync.Mutex
	queue  []chan error
	notify chan struct{}

	wg sync.WaitGroup
}

func NewFileWriter(ws writeSyncer, skipSync bool) *FileWriter {
	fs := &FileWriter{
		ws:       ws,
		skipSync: skipSync,
		notify:   make(chan struct{}, 1),
	}

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

func (fs *FileWriter) Write(data []byte, sw *stopwatch.Stopwatch) error {
	m := sw.Start("write_duration")
	_, err := fs.ws.Write(data)
	m.Stop()

	if err != nil {
		return err
	}

	if fs.skipSync {
		return nil
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

	return err
}

func (fs *FileWriter) Stop() {
	close(fs.notify)
	fs.wg.Wait()
}
