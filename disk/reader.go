package disk

import (
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/conf"
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
	in     chan *readTask
	metric prometheus.Counter
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

func (r *Reader) ReadDocBlock(f *os.File, offset int64) (bytespool.ReleasableBytes, uint64, error) {
	l, err := r.GetDocBlockLen(f, offset)
	if err != nil {
		return bytespool.ReleasableBytes{}, 0, err
	}

	block := bytespool.NewReleasableBytes(l)
	n, err := r.readDocBlockInBuf(f, offset, block.Buf.B)
	return block, n, err
}

func (r *Reader) ReadDocBlockPayload(f *os.File, offset int64) (bytespool.ReleasableBytes, uint64, error) {
	block, n, err := r.ReadDocBlock(f, offset)
	defer block.Release()

	if err != nil {
		return bytespool.ReleasableBytes{}, 0, err
	}

	// decompress
	docBlock := DocBlock(block.Value())
	dst := bytespool.NewReleasableBytes(docBlock.RawLen())
	dst.Buf.B, err = docBlock.DecompressTo(dst.Buf.B)
	if err != nil {
		return bytespool.ReleasableBytes{}, 0, err
	}

	return dst, n, nil
}

func (r *Reader) readDocBlockInBuf(f *os.File, offset int64, buf []byte) (uint64, error) {
	task := &ReadDocTask{
		f:      f,
		offset: offset,
		Buf:    buf,
	}
	r.process((*readTask)(task))
	return task.N, task.Err
}

func (r *Reader) GetDocBlockLen(f *os.File, offset int64) (uint64, error) {
	task := &ReadDocTask{
		f:      f,
		offset: offset,
	}

	buf := bytespool.Acquire(DocBlockHeaderLen)
	defer bytespool.Release(buf)

	task.Buf = buf.B
	r.process((*readTask)(task))
	if task.Err != nil {
		return 0, task.Err
	}

	return DocBlock(task.Buf).FullLen(), nil
}

func (r *Reader) ReadIndexBlock(blocksReader *BlocksReader, blockIndex uint32) (_ bytespool.ReleasableBytes, _ uint64, err error) {
	header, err := blocksReader.GetBlockHeader(blockIndex)
	if err != nil {
		return bytespool.ReleasableBytes{}, 0, err
	}

	task := &ReadIndexTask{
		f:      blocksReader.openedFile(),
		offset: int64(header.GetPos()),
	}

	if header.Codec() == CodecNo {
		dst := bytespool.NewReleasableBytes(uint64(header.Len()))
		task.Buf = dst.Buf.B
		r.process((*readTask)(task))
		return dst, task.N, task.Err
	}

	readBuf := bytespool.Acquire(int(header.Len()))
	defer bytespool.Release(readBuf)

	task.Buf = readBuf.B
	r.process((*readTask)(task))

	if task.Err != nil {
		return bytespool.ReleasableBytes{}, task.N, task.Err
	}

	dst := bytespool.NewReleasableBytes(uint64(header.RawLen()))
	dst.Buf.B, err = header.Codec().decompressBlock(int(header.RawLen()), task.Buf, dst.Buf.B)

	return dst, task.N, err
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
