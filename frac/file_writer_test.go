package frac

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
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

type testRandPauseWriterAt struct {
	f *os.File
}

func (w *testRandPauseWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	// random pause
	time.Sleep(time.Microsecond * time.Duration(rand.IntN(20)))
	return w.f.WriteAt(p, off)
}

func (w *testRandPauseWriterAt) Sync() error {
	return w.f.Sync()
}

func TestConcurrentFileWriting(t *testing.T) {
	f, e := os.Create(t.TempDir() + "/test.txt")
	assert.NoError(t, e)

	defer f.Close()

	fw := NewFileWriter(&testRandPauseWriterAt{f: f}, 0, true)

	const (
		writersCount = 100
		writesCount  = 1000
	)

	type writeSample struct {
		offset int64
		data   []byte
	}

	wg := sync.WaitGroup{}
	samplesQueues := [writersCount][]writeSample{}

	// run writers
	for i := range writersCount {
		wg.Add(1)
		go func() {
			defer wg.Done()

			sw := stopwatch.New()
			workerName := strconv.Itoa(i)

			for j := range writesCount {

				data := []byte("<" + workerName + "-" + strconv.Itoa(j) + ">")
				offset, e := fw.Write(data, sw)
				assert.NoError(t, e)

				samplesQueues[i] = append(samplesQueues[i], writeSample{data: data, offset: offset})
			}
		}()
	}

	wg.Wait()

	// join and sort all samples by offset
	all := make([]writeSample, 0, writersCount*writersCount)
	for _, c := range samplesQueues {
		all = append(all, c...)
	}
	slices.SortFunc(all, func(a, b writeSample) int {
		if a.offset < b.offset {
			return -1
		}
		if a.offset > b.offset {
			return 1
		}
		return 0
	})

	// check all samples and file content
	offset := int64(0)
	buf := make([]byte, 1000)
	for _, w := range all {
		s := len(w.data)
		buf = buf[:s]
		_, e = f.ReadAt(buf, int64(offset))
		assert.NoError(t, e)
		assert.Equal(t, w.data, buf)
		assert.Equal(t, w.offset, offset)
		offset += int64(s)
	}

	s, e := f.Stat()
	assert.NoError(t, e)
	fmt.Println(s.Size())

	assert.Equal(t, offset, s.Size())

	e = os.Remove(f.Name())
	assert.NoError(t, e)
}

func TestSparseWrite(t *testing.T) {
	wf, e := os.Create(t.TempDir() + "/test.txt")
	assert.NoError(t, e)

	_, e = wf.WriteAt([]byte("333"), 30)
	assert.NoError(t, e)

	_, e = wf.WriteAt([]byte("222"), 20)
	assert.NoError(t, e)

	_, e = wf.WriteAt([]byte("111"), 10)
	assert.NoError(t, e)

	e = wf.Close()
	assert.NoError(t, e)

	rf, e := os.Open(wf.Name())
	buf := make([]byte, 33)
	assert.NoError(t, e)

	n, e := rf.Read(buf)
	assert.NoError(t, e)
	assert.Equal(t, len(buf), n)

	expected := []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00111\x00\x00\x00\x00\x00\x00\x00222\x00\x00\x00\x00\x00\x00\x00333")
	assert.Equal(t, expected, buf)

	n, e = rf.Read(buf)
	assert.Error(t, e)
	assert.Equal(t, 0, n)
	assert.ErrorIs(t, e, io.EOF)

	e = rf.Close()
	assert.NoError(t, e)

	e = os.Remove(rf.Name())
	assert.NoError(t, e)
}
