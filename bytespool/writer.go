package bytespool

import (
	"io"
	"sync"
	"unsafe"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
)

var writerPool = sync.Pool{}

type Writer struct {
	Buf *Buffer
	out io.Writer
}

func AcquireWriterSize(out io.Writer, size int) *Writer {
	if w := writerPool.Get(); w != nil {
		w := w.(*Writer)
		w.out = out
		w.Buf = AcquireReset(size)
		return w
	}

	return &Writer{
		out: out,
		Buf: AcquireReset(size),
	}
}

func FlushReleaseWriter(w *Writer) {
	err := w.Flush()
	if err != nil {
		logger.Error("can't flush bytespool.Writer", zap.Error(err))
	}
	Release(w.Buf)
	w.Buf = nil
	w.out = nil
	writerPool.Put(w)
}

func (w *Writer) Write(b []byte) (int, error) {
	if len(w.Buf.B)+len(b) < cap(w.Buf.B) {
		// We fit into buffer
		w.Buf.B = append(w.Buf.B, b...)
		return len(b), nil
	}

	if len(w.Buf.B) == 0 {
		// No need to flush, write directly
		return w.writeCheckShort(b)
	}

	// Buffer is not empty, fill and flush it
	n := cap(w.Buf.B) - len(w.Buf.B)
	w.Buf.B = append(w.Buf.B, b[:n]...)
	b = b[n:]
	if err := w.Flush(); err != nil {
		return 0, err
	}

	// Check we can fit into buffer
	if len(b) < cap(w.Buf.B) {
		w.Buf.B = append(w.Buf.B, b...)
		return n + len(b), nil
	}
	// Buffer is empty, and we can't append byte slice, so write directly
	return w.writeCheckShort(b)
}

func (w *Writer) WriteString(s string) (int, error) {
	return w.Write(unsafe.Slice(unsafe.StringData(s), len(s)))
}

func (w *Writer) Flush() error {
	if len(w.Buf.B) == 0 {
		return nil
	}

	_, err := w.writeCheckShort(w.Buf.B)
	if err != nil {
		return err
	}
	w.Buf.Reset()
	return nil
}

func (w *Writer) writeCheckShort(b []byte) (int, error) {
	n, err := w.out.Write(b)
	if err != nil {
		return n, err
	}
	if n != len(b) {
		return n, io.ErrShortWrite
	}
	return n, nil
}
