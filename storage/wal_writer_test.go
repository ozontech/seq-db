package storage

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

	"github.com/alecthomas/units"
	"github.com/stretchr/testify/assert"

	"github.com/ozontech/seq-db/metric/stopwatch"
)

type testWriterSyncer struct {
	mu    sync.RWMutex
	in    [][]byte
	out   map[string]struct{}
	pause time.Duration
	err   bool
	bytes []byte
}

func TestFileWriter(t *testing.T) {
	ws := &testWriterSyncer{out: map[string]struct{}{}, pause: time.Millisecond}
	fw := NewWalWriter(ws, 0, false)

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
	fw := NewWalWriter(ws, 0, true)

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
	fw := NewWalWriter(ws, 0, false)

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

func (ws *testWriterSyncer) WriteAt(p []byte, off int64) (n int, err error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.in = append(ws.in, p)

	// Extend storage if needed and write data
	end := int(off) + len(p)
	if end > len(ws.bytes) {
		newStorage := make([]byte, end)
		copy(newStorage, ws.bytes)
		ws.bytes = newStorage
	}
	copy(ws.bytes[off:], p)

	return len(p), nil
}

func (ws *testWriterSyncer) ReadAt(p []byte, off int64) (n int, err error) {
	ws.mu.RLock()
	defer ws.mu.RUnlock()

	if int(off) >= len(ws.bytes) {
		return 0, io.EOF
	}

	n = copy(p, ws.bytes[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
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

type testRandPauseWriterAt struct {
	f *os.File
}

func (w *testRandPauseWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	// random pause
	time.Sleep(time.Microsecond * time.Duration(rand.IntN(20)))
	return w.f.WriteAt(p, off)
}

func (w *testRandPauseWriterAt) ReadAt(p []byte, off int64) (n int, err error) {
	return w.f.ReadAt(p, off)
}

func (w *testRandPauseWriterAt) Sync() error {
	return w.f.Sync()
}

func TestConcurrentFileWriting(t *testing.T) {
	f, e := os.Create(t.TempDir() + "/test.txt")
	assert.NoError(t, e)

	defer f.Close()

	fw := NewWalWriter(&testRandPauseWriterAt{f: f}, 0, true)

	const (
		writersCount = 100
		writesCount  = 100
	)

	type writeSample struct {
		offset  int64
		payload []byte
	}

	wg := sync.WaitGroup{}
	samplesQueues := [writersCount][]writeSample{}

	// run writers - write MetaBlocks
	for i := range writersCount {
		wg.Add(1)
		go func() {
			defer wg.Done()

			sw := stopwatch.New()
			workerName := strconv.Itoa(i)

			for j := range writesCount {
				payload := []byte("<" + workerName + "-" + strconv.Itoa(j) + ">")
				metaBlock := PackMetaBlock(payload, nil)
				offset, e := fw.Write(metaBlock, sw)
				assert.NoError(t, e)

				samplesQueues[i] = append(samplesQueues[i], writeSample{payload: payload, offset: offset})
			}
		}()
	}

	wg.Wait()

	// join and sort all samples by offset
	all := make([]writeSample, 0, writersCount*writesCount)
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

	reader := NewWalReader(NewReadLimiter(1, nil), f, "")
	idx := 0
	for entry := range reader.Iter() {
		assert.Equal(t, all[idx].offset, entry.Offset, "block %d offset mismatch", idx)
		assert.Equal(t, all[idx].payload, entry.Data.Payload(), "block %d payload mismatch", idx)
		idx++
	}
	assert.Equal(t, len(all), idx, "should read all blocks")

	s, e := f.Stat()
	assert.NoError(t, e)
	fmt.Printf("File size: %d bytes, %d blocks written\n", s.Size(), len(all))

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

func TestWalWriterWriteAndRead(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-test-*.bin")
	assert.NoError(t, err)
	defer f.Close()

	fw := NewWalWriter(f, 0, false)

	payloads := [][]byte{
		[]byte("block one"),
		[]byte("block two with more data"),
		[]byte("block three"),
		[]byte("fourth block here"),
		[]byte("and the fifth block"),
	}

	offsets := make([]int64, len(payloads))
	sw := stopwatch.New()

	for i, payload := range payloads {
		metaBlock := PackMetaBlock(payload, nil)
		offset, err := fw.Write(metaBlock, sw)
		assert.NoError(t, err)
		offsets[i] = offset
	}

	fw.Stop()

	reader := NewWalReader(NewReadLimiter(1, nil), f, "")
	count := 0
	for entry := range reader.Iter() {
		assert.Equal(t, offsets[count], entry.Offset, "block %d offset mismatch", count)
		assert.Equal(t, payloads[count], entry.Data.Payload(), "block %d payload mismatch", count)
		assert.Equal(t, MetaBlockMagic, entry.Data.Magic(), "block %d should have MetaBlock magic", count)
		count++
	}
	assert.Equal(t, len(payloads), count, "should read all blocks")
}

func TestWalReaderIteratorEmptyFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-iter-*.bin")
	assert.NoError(t, err)
	defer f.Close()

	fw := NewWalWriter(f, 0, false)
	fw.Stop()

	reader := NewWalReader(NewReadLimiter(1, nil), f, "")

	count := 0
	for range reader.Iter() {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestWalReaderIterator(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-iter-*.bin")
	assert.NoError(t, err)
	defer f.Close()

	fw := NewWalWriter(f, 0, false)

	payloads := [][]byte{
		[]byte("first payload data"),
		[]byte("second payload with more content here"),
		[]byte("third"),
		[]byte("fourth payload block"),
		[]byte("fifth and final payload"),
	}

	sw := stopwatch.New()
	var expectedOffsets []int64

	for _, payload := range payloads {
		metaBlock := PackMetaBlock(payload, nil)
		offset, err := fw.Write(metaBlock, sw)
		assert.NoError(t, err)
		expectedOffsets = append(expectedOffsets, offset)
	}
	fw.Stop()

	reader := NewWalReader(NewReadLimiter(1, nil), f, "")

	var readPayloads [][]byte
	var readOffsets []int64
	idx := 0
	for entry := range reader.Iter() {
		readPayloads = append(readPayloads, entry.Data.Payload())
		readOffsets = append(readOffsets, entry.Offset)
		idx++
	}

	assert.Equal(t, len(payloads), len(readPayloads))
	for i, expected := range payloads {
		assert.Equal(t, expected, readPayloads[i], "block %d payload doesn't match", i)
		assert.Equal(t, expectedOffsets[i], readOffsets[i], "block %d offset doesn't match", i)
	}
}

// TestWalReaderSkipsCorruptedBlocks tests very simple single byte corruption in block header
func TestWalReaderSkipsCorruptedBlocks(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-corrupted-*.bin")
	assert.NoError(t, err)
	defer f.Close()

	fw := NewWalWriter(f, 0, false)

	payloads := [][]byte{
		[]byte("block one"),
		[]byte("block two - will be corrupted"),
		[]byte("block three"),
		[]byte("block four"),
	}

	sw := stopwatch.New()
	var offsets []int64

	for _, payload := range payloads {
		metaBlock := PackMetaBlock(payload, nil)
		offset, err := fw.Write(metaBlock, sw)
		assert.NoError(t, err)
		offsets = append(offsets, offset)
	}
	fw.Stop()

	// corrupt block 2 (index 1) by flipping a byte in the header checksum
	corruptOffset := offsets[1] + offsetMetaBlockHeaderChecksum
	_, err = f.WriteAt([]byte{0xFF}, corruptOffset)
	assert.NoError(t, err)
	t.Logf("corrupted header checksum at offset %d", corruptOffset)

	reader := NewWalReader(NewReadLimiter(1, nil), f, "")

	var readPayloads [][]byte
	for entry := range reader.Iter() {
		readPayloads = append(readPayloads, entry.Data.Payload())
		t.Logf("read block at offset %d: %q", entry.Offset, entry.Data.Payload())
	}

	assert.Equal(t, 3, len(readPayloads), "should recover 3 out of 4 blocks")
	assert.Equal(t, payloads[0], readPayloads[0], "first block should match")
	assert.Equal(t, payloads[2], readPayloads[1], "third block should match")
	assert.Equal(t, payloads[3], readPayloads[2], "fourth block should match")
}

// TestWalReaderSkipsCorruptedPayload tests very simple single byte corruption
func TestWalReaderSkipsCorruptedPayload(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal-payload-corrupt-*.bin")
	assert.NoError(t, err)
	defer f.Close()

	fw := NewWalWriter(f, 0, false)

	payloads := [][]byte{
		[]byte("first block"),
		[]byte("second block - payload will be corrupted"),
		[]byte("third block"),
	}

	sw := stopwatch.New()
	var offsets []int64

	for _, payload := range payloads {
		metaBlock := PackMetaBlock(payload, nil)
		offset, err := fw.Write(metaBlock, sw)
		assert.NoError(t, err)
		offsets = append(offsets, offset)
	}
	fw.Stop()

	payloadOffset := offsets[1] + MetaBlockHeaderLen + 5 // corrupt somewhere in payload
	_, err = f.WriteAt([]byte{0xFF, 0xFF}, payloadOffset)
	assert.NoError(t, err)

	reader := NewWalReader(NewReadLimiter(1, nil), f, "")

	var readPayloads [][]byte
	for entry := range reader.Iter() {
		readPayloads = append(readPayloads, entry.Data.Payload())
	}

	assert.Equal(t, 2, len(readPayloads), "should recover 2 out of 3 blocks")
	assert.Equal(t, payloads[0], readPayloads[0], "first block should match")
	assert.Equal(t, payloads[2], readPayloads[1], "third block should match")
}

// TestWalReaderSingleByteCorruption tests flipping a random byte in WAL file and verifying that we never
// lose more than a single block on replay
func TestWalReaderSingleByteCorruption(t *testing.T) {
	const (
		numRuns       = 100
		numBlocks     = 100
		minPayloadLen = 10
		maxPayloadLen = int(10 * units.KiB)
	)

	totalLostBlocks := 0

	for run := range numRuns {
		f, err := os.CreateTemp(t.TempDir(), fmt.Sprintf("wal-corruption-%d-*.bin", run))
		assert.NoError(t, err)

		fw := NewWalWriter(f, 0, false)
		sw := stopwatch.New()

		blocks := make([]MetaBlock, 0)

		// write blocks to WAL
		for i := range numBlocks {
			payloadLen := minPayloadLen + rand.IntN(maxPayloadLen-minPayloadLen+1)
			payload := make([]byte, payloadLen)

			for j := range payload {
				payload[j] = byte(rand.IntN(256))
			}
			// store the first byte as index of block
			payload[0] = byte(i)

			metaBlock := PackMetaBlock(payload, nil)
			blocks = append(blocks, metaBlock)
			_, err = fw.Write(metaBlock, sw)
			assert.NoError(t, err)
		}
		fw.Stop()

		stat, err := f.Stat()
		assert.NoError(t, err)
		fileSize := stat.Size()

		// flip a random byte at random offset
		// we do not corrupt the first 5 bytes - WAL header, other bytes might be corrupted including block headers
		corruptOffset := int64(WALHeaderSize) + rand.Int64N(fileSize-WALHeaderSize)

		originalByte := make([]byte, 1)
		_, err = f.ReadAt(originalByte, corruptOffset)
		assert.NoError(t, err)
		corruptedByte := originalByte[0] ^ 0xFF
		_, err = f.WriteAt([]byte{corruptedByte}, corruptOffset)
		assert.NoError(t, err)

		reader := NewWalReader(NewReadLimiter(1, nil), f, "")

		readBlocks := 0

		for entry := range reader.Iter() {
			assert.NoError(t, entry.Err)
			assert.True(t, entry.Data.IsCorrect())

			expected := blocks[int(entry.Data.Payload()[0])]
			assert.Equal(t, expected, entry.Data, "meta block content doesn't match")
			readBlocks++
		}

		lostCount := numBlocks - readBlocks
		totalLostBlocks += lostCount

		if lostCount > 1 {
			assert.Fail(t, "lost %d blocks", lostCount)
		}

		err = f.Close()
		assert.NoError(t, err)
	}

	assert.NotZero(t, totalLostBlocks, "have not missed a single block across 100 runs")
}

// TestWalReaderTruncation tests that we can iterate through truncated WAL file.
func TestWalReaderTruncation(t *testing.T) {
	const (
		numRuns       = 100
		numBlocks     = 100
		minPayloadLen = 512
		maxPayloadLen = int(10 * units.KiB)
	)

	for run := range numRuns {
		f, err := os.CreateTemp(t.TempDir(), fmt.Sprintf("wal-truncate-%d-*.bin", run))
		assert.NoError(t, err)

		fw := NewWalWriter(f, 0, false)
		sw := stopwatch.New()

		blocks := make([]MetaBlock, 0)
		offsets := make([]int64, 0)

		// write blocks to WAL
		for i := range numBlocks {
			payloadLen := minPayloadLen + rand.IntN(maxPayloadLen-minPayloadLen+1)
			payload := make([]byte, payloadLen)

			for j := range payload {
				payload[j] = byte(rand.IntN(256))
			}
			// store the first byte as index of block
			payload[0] = byte(i)

			metaBlock := PackMetaBlock(payload, nil)
			blocks = append(blocks, metaBlock)
			offset, err := fw.Write(metaBlock, sw)
			assert.NoError(t, err)
			offsets = append(offsets, offset)
		}
		fw.Stop()

		// choose random block index at which truncation happens
		truncateIndex := rand.IntN(numBlocks)

		// this ensures truncation happens somewhere within the chosen block either at the header or the payload
		truncateOffset := offsets[truncateIndex] + rand.Int64N(50)

		err = f.Truncate(truncateOffset)
		assert.NoError(t, err)

		// validate we can read all blocks from 0 to truncateIndex (exclusive)
		reader := NewWalReader(NewReadLimiter(1, nil), f, "")

		readBlocks := 0

		for entry := range reader.Iter() {
			assert.NoError(t, entry.Err)
			assert.True(t, entry.Data.IsCorrect())

			// verify block index matches expected sequence
			blockIndex := int(entry.Data.Payload()[0])
			assert.Equal(t, readBlocks, blockIndex)

			expected := blocks[blockIndex]
			assert.Equal(t, expected, entry.Data)
			readBlocks++
		}

		// we should have read exactly truncateIndex blocks
		assert.Equal(t, truncateIndex, readBlocks)

		err = f.Close()
		assert.NoError(t, err)
	}
}

// TestWalReaderSectorLoss tests losing whole disk sectors (512-byte each)
func TestWalReaderSectorLoss(t *testing.T) {
	const (
		numRuns          = 100
		numBlocks        = 100
		minPayloadLen    = 128
		maxPayloadLen    = int(4 * units.KiB)
		sectorSize       = 512
		sectorsToCorrupt = 10
		// each disk sector spans 4 blocks at most (each block is at least 128 bytes long)
		maxLostBlocks = sectorsToCorrupt * 4
	)

	for run := range numRuns {
		f, err := os.CreateTemp(t.TempDir(), fmt.Sprintf("wal-sector-loss-%d-*.bin", run))
		assert.NoError(t, err)

		fw := NewWalWriter(f, 0, false)
		sw := stopwatch.New()

		blocks := make([]MetaBlock, 0)

		// write blocks to WAL
		for i := range numBlocks {
			payloadLen := minPayloadLen + rand.IntN(maxPayloadLen-minPayloadLen+1)
			payload := make([]byte, payloadLen)

			for j := range payload {
				payload[j] = byte(rand.IntN(256))
			}
			// store the first byte as index of block
			payload[0] = byte(i)

			metaBlock := PackMetaBlock(payload, nil)
			blocks = append(blocks, metaBlock)
			_, err = fw.Write(metaBlock, sw)
			assert.NoError(t, err)
		}
		fw.Stop()

		stat, err := f.Stat()
		assert.NoError(t, err)
		fileSize := stat.Size()

		numSectors := int(fileSize / sectorSize)
		zeroes := make([]byte, sectorSize)
		corruptedSectors := make(map[int]bool)

		// zero out 10 random sectors (not the first one which contains WAL header)
		for len(corruptedSectors) < sectorsToCorrupt && len(corruptedSectors) < numSectors-1 {
			// choose sector index from 1 to numSectors-1 (skip first sector)
			idx := 1 + rand.IntN(numSectors-1)
			if corruptedSectors[idx] {
				continue
			}
			corruptedSectors[idx] = true

			sectorOffset := int64(idx * sectorSize)
			_, err = f.WriteAt(zeroes, sectorOffset)
			assert.NoError(t, err)
		}

		reader := NewWalReader(NewReadLimiter(1, nil), f, "")
		readBlocks := 0

		for entry := range reader.Iter() {
			assert.NoError(t, entry.Err)
			assert.True(t, entry.Data.IsCorrect())

			// validate payload content
			blockIndex := int(entry.Data.Payload()[0])
			expected := blocks[blockIndex]
			assert.Equal(t, expected, entry.Data)
			readBlocks++
		}

		lostCount := numBlocks - readBlocks
		if lostCount > maxLostBlocks {
			assert.Fail(t, "lost too much blocks")
		}

		err = f.Close()
		assert.NoError(t, err)
	}
}
