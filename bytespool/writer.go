package bytespool

import (
	"io"
	"sync"
	"unsafe"
)

var writerPool = sync.Pool{}

// Writer is a buffered writer that can use a provided byte slice, unlike bufio.Writer.
// This can be useful directly appending to an existing byte slice (for example strconv.AppendUint).
type Writer struct {
	Buf *Buffer
	out io.Writer
}

func AcquireWriterSize(out io.Writer, size int) *Writer {
	if w := writerPool.Get(); w != nil {
		w := w.(*Writer)
		w.out = out
		w.Buf = Acquire(size)
		return w
	}

	return &Writer{
		out: out,
		Buf: Acquire(size),
	}
}

func ReleaseWriter(w *Writer) {
	Release(w.Buf)
	w.Buf = nil
	w.out = nil
	writerPool.Put(w)
}

func FlushReleaseWriter(w *Writer) error {
	if err := w.Flush(); err != nil {
		return err
	}
	ReleaseWriter(w)
	return nil
}

func (w *Writer) Write(b []byte) (int, error) {
	n := len(b)
	if len(w.Buf.B)+len(b) < cap(w.Buf.B) {
		// We fit into buffer
		w.Buf.B = append(w.Buf.B, b...)
		return n, nil
	}

	if len(w.Buf.B) == 0 {
		// No need to flush, write directly
		err := w.writeCheckShort(b)
		return n, err
	}

	// Buffer is not empty, fill and flush it
	v := cap(w.Buf.B) - len(w.Buf.B)
	w.Buf.B = append(w.Buf.B, b[:v]...)
	b = b[v:]
	if err := w.Flush(); err != nil {
		return 0, err
	}

	// Check we can fit into buffer
	if len(b) < cap(w.Buf.B) {
		w.Buf.B = append(w.Buf.B, b...)
		return n, nil
	}
	// Buffer is empty, and we can't append byte slice, so write directly
	err := w.writeCheckShort(b)
	return n, err
}

func (w *Writer) WriteString(s string) (int, error) {
	return w.Write(unsafe.Slice(unsafe.StringData(s), len(s)))
}

func (w *Writer) Flush() error {
	if len(w.Buf.B) == 0 {
		return nil
	}
	if err := w.writeCheckShort(w.Buf.B); err != nil {
		return err
	}
	w.Buf.Reset()
	return nil
}

func (w *Writer) writeCheckShort(b []byte) error {
	n, err := w.out.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}
