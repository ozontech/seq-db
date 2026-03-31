package sealing

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/alecthomas/units"
	"github.com/ozontech/seq-db/bytespool"
)

const prefixSize = 16

// writer writes blocks incrementally to a single file using the
// [prefix][blocks][registry] format.
type writer struct {
	ws io.WriteSeeker

	wpayload *bytespool.Writer
	wheader  bytes.Buffer

	pos int
}

func newWriter(ws io.WriteSeeker) (*writer, error) {
	if _, err := ws.Seek(prefixSize, io.SeekStart); err != nil {
		return nil, err
	}

	return &writer{
		ws:       ws,
		wpayload: bytespool.AcquireWriterSize(ws, int(units.MiB)),
		pos:      prefixSize,
	}, nil
}

func (w *writer) writeBlock(block indexBlock) error {
	header, payload := block.Bin(int64(w.pos))

	if _, err := w.wpayload.Write(payload); err != nil {
		return err
	}

	w.wheader.Write(header)
	w.pos += len(payload)

	return nil
}

func (w *writer) finalize() error {
	if err := w.wpayload.Flush(); err != nil {
		return err
	}

	regpos, err := w.ws.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if _, err := w.wpayload.Write(w.wheader.Bytes()); err != nil {
		return err
	}

	if err := w.wpayload.Flush(); err != nil {
		return err
	}

	prefix := make([]byte, 0, prefixSize)
	prefix = binary.LittleEndian.AppendUint64(prefix, uint64(regpos))
	prefix = binary.LittleEndian.AppendUint64(prefix, uint64(w.wheader.Len()))

	if _, err := w.ws.Seek(0, io.SeekStart); err != nil {
		return err
	}

	_, err = w.ws.Write(prefix)
	return err
}

func (w *writer) release() {
	bytespool.ReleaseWriter(w.wpayload)
	w.wpayload = nil
}
