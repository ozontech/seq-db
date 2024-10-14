package disk

import (
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/util"
)

type readTask struct {
	Err error
	Buf []byte
	N   uint64

	// internal
	f      *os.File
	offset int64
	wg     sync.WaitGroup
}

type ReadDocTask readTask

type ReadIndexTask readTask

type Reader struct {
	in          chan *readTask
	readBufPool sync.Pool
	metric      prometheus.Counter
}

func NewReader(counter prometheus.Counter) *Reader {
	r := &Reader{
		in:     make(chan *readTask),
		metric: counter,
	}
	for i := 0; i < conf.ReaderWorkers; i++ {
		go r.readWorker()
	}
	return r
}

func (r *Reader) process(task *readTask) {
	task.wg.Add(1)
	r.in <- task
	task.wg.Wait()
}

// ReadDocBlock attempts to reuse outBuf as ReadDocTask.Buf
// thus it should be copied before storing unless outBuf was nil
func (r *Reader) ReadDocBlock(f *os.File, offset int64, length uint64, outBuf []byte) *ReadDocTask {
	task := &ReadDocTask{
		f:      f,
		offset: offset,
	}

	if length == 0 {
		// determine len
		task.Buf = util.EnsureSliceSize(outBuf, DocBlockHeaderLen)
		r.process((*readTask)(task))
		if task.Err != nil {
			return task
		}
		// for the sake of simplicity we read the whole block again
		length = DocBlock(task.Buf).FullLen()
		outBuf = task.Buf
	}

	task.Buf = util.EnsureSliceSize(outBuf, int(length))
	r.process((*readTask)(task))
	return task
}

// ReadIndexBlock attempts to reuse outBuf as ReadIndexTask.Buf
// thus it should be copied before storing unless outBuf was nil
func (r *Reader) ReadIndexBlock(blocksReader *BlocksReader, blockIndex uint32, outBuf []byte) (task *ReadIndexTask) {
	task = &ReadIndexTask{}

	// actually, it protects only GetBlockHeader
	defer func() {
		if e := util.RecoverToError(recover(), metric.StorePanics); e != nil {
			task.Err = e
		}
	}()

	header := blocksReader.GetBlockHeader(blockIndex)

	// always succeeds after GetBlockHeader
	task.f = blocksReader.tryOpenFile()
	task.offset = int64(header.GetPos())

	if header.Codec() == CodecNo {
		task.Buf = util.EnsureSliceSize(outBuf, int(header.Len()))
		r.process((*readTask)(task))
		return
	}

	readBuf := r.readBufPool.Get()
	defer func() { r.readBufPool.Put(readBuf) }()

	if readBuf != nil {
		task.Buf = readBuf.([]byte)
	}
	task.Buf = util.EnsureSliceSize(task.Buf, int(header.Len()))
	r.process((*readTask)(task))
	readBuf = task.Buf

	if task.Err != nil {
		task.Buf = outBuf[:0]
		return
	}

	task.Buf, task.Err = header.Codec().decompressBlock(int(header.RawLen()), task.Buf, outBuf)

	return
}

// Decompress always makes a copy of ReadDocTask.Buf
// thus it shouldn't be copied again
func (task *ReadDocTask) Decompress() {
	block := DocBlock(task.Buf)

	if block.Codec() == CodecNo {
		task.Buf = append([]byte{}, block.Payload()...)
		return
	}

	task.Buf, task.Err = block.Codec().decompressBlock(int(block.RawLen()), block.Payload(), nil)
}

func (r *Reader) Stop() {
	close(r.in)
}

func (r *Reader) readWorker() {
	for task := range r.in {
		r.readBlock(task)
	}
}

func (r *Reader) readBlock(task *readTask) {
	defer task.wg.Done()

	var n int
	n, task.Err = task.f.ReadAt(task.Buf, task.offset)
	task.N = uint64(n)

	if r.metric != nil {
		r.metric.Add(float64(n))
	}
}
