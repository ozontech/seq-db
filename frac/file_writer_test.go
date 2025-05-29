package frac

import (
	"errors"
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ozontech/seq-db/metric/stopwatch"
)

type testWriterSyncer struct {
	mu    sync.RWMutex
	in    [][]byte
	out   map[string]struct{}
	pause time.Duration
	err   bool
}

func (ws *testWriterSyncer) WriteAt(p []byte, _ int64) (n int, err error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.in = append(ws.in, p)

	return len(p), nil
}

func (ws *testWriterSyncer) Sync() error {
	time.Sleep(ws.pause)

	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.err {
		ws.in = nil
		return errors.New("test")
	}

	for _, val := range ws.in {
		ws.out[string(val)] = struct{}{}
	}
	ws.in = nil

	return nil
}

func (ws *testWriterSyncer) Check(val []byte) bool {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	_, ok := ws.out[string(val)]
	return ok
}

func TestFileWriter(t *testing.T) {
	ws := &testWriterSyncer{out: map[string]struct{}{}, pause: time.Millisecond}
	fw := NewFileWriter(ws, 0, false)

	wg := sync.WaitGroup{}
	for range 100 {
		wg.Add(1)
		go func() {
			for range 100 {
				sw := stopwatch.New()
				k := []byte(strconv.FormatUint(rand.Uint64(), 16))
				_, err := fw.Write(k, sw)
				assert.NoError(t, err)
				assert.True(t, ws.Check(k))
			}
			wg.Done()
		}()
	}

	wg.Wait()
	fw.Stop()
}

func TestFileWriterNoSync(t *testing.T) {
	ws := &testWriterSyncer{out: map[string]struct{}{}, pause: time.Millisecond}
	fw := NewFileWriter(ws, 0, true)

	wg := sync.WaitGroup{}
	for range 100 {
		wg.Add(1)
		go func() {
			for range 100 {
				sw := stopwatch.New()
				k := []byte(strconv.FormatUint(rand.Uint64(), 16))
				_, err := fw.Write(k, sw)
				assert.NoError(t, err)
				assert.False(t, ws.Check(k))
			}
			wg.Done()
		}()
	}

	wg.Wait()
	fw.Stop()
}

func TestFileWriterError(t *testing.T) {
	ws := &testWriterSyncer{out: map[string]struct{}{}, pause: time.Millisecond, err: true}
	fw := NewFileWriter(ws, 0, false)

	wg := sync.WaitGroup{}
	for range 100 {
		wg.Add(1)
		go func() {
			for range 100 {
				sw := stopwatch.New()
				k := []byte(strconv.FormatUint(rand.Uint64(), 16))
				_, err := fw.Write(k, sw)
				assert.Error(t, err)
				assert.False(t, ws.Check(k))
			}
			wg.Done()
		}()
	}

	wg.Wait()
	fw.Stop()
}
