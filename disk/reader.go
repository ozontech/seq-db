package disk

import (
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ozontech/seq-db/bytespool"
	"github.com/ozontech/seq-db/conf"
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

func (r *Reader) ReadDocBlock(f *os.File, offset int64) ([]byte, uint64, error) {
	l, err := r.GetDocBlockLen(f, offset)
	if err != nil {
		return nil, 0, err
	}
	return r.readDocBlockInBuf(f, offset, make([]byte, l))
}

func (r *Reader) ReadDocBlockPayload(f *os.File, offset int64) ([]byte, uint64, error) {
	l, err := r.GetDocBlockLen(f, offset)
	if err != nil {
		return nil, 0, err
	}

	buf := bytespool.Acquire(int(l))
	defer bytespool.Release(buf)

	var n uint64
	buf.B, n, err = r.readDocBlockInBuf(f, offset, buf.B)
	if err != nil {
		return nil, 0, err
	}

	// decompress
	docBlock := DocBlock(buf.B)
	dst, err := docBlock.DecompressTo(make([]byte, docBlock.RawLen()))
	if err != nil {
		return nil, 0, err
	}

	return dst, n, nil
}

func (r *Reader) readDocBlockInBuf(f *os.File, offset int64, buf []byte) ([]byte, uint64, error) {
	task := &ReadDocTask{
		f:      f,
		offset: offset,
		Buf:    buf,
	}
	r.process((*readTask)(task))
	return task.Buf, task.N, task.Err
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

func (r *Reader) ReadIndexBlock(blocksReader *RegistryReader, blockIndex uint32, dst []byte) (_ []byte, _ uint64, err error) {
	header, err := blocksReader.TryGetBlockHeader(blockIndex)
	if err != nil {
		return nil, 0, err
	}

	task := &ReadIndexTask{
		f:      blocksReader.File(),
		offset: int64(header.GetPos()),
	}

	if header.Codec() == CodecNo {
		dst := make([]byte, header.Len())
		task.Buf = dst
		r.process((*readTask)(task))
		return dst, task.N, task.Err
	}

	readBuf := bytespool.Acquire(int(header.Len()))
	defer bytespool.Release(readBuf)

	task.Buf = readBuf.B
	r.process((*readTask)(task))

	if task.Err != nil {
		return nil, task.N, task.Err
	}

	dst = util.EnsureSliceSize(dst, int(header.RawLen()))
	dst, err = header.Codec().decompressBlock(int(header.RawLen()), task.Buf, dst)

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
