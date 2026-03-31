package sealing

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/alecthomas/units"
	"github.com/ozontech/seq-db/bytespool"
)

const filePrefixSize = 16

// fileStreamWriter writes blocks incrementally to a single file using the
// [prefix][blocks][registry] format, allowing interleaved writes to multiple files.
type fileStreamWriter struct {
	ws  io.WriteSeeker
	bw  *bytespool.Writer
	hw  bytes.Buffer
	pos int
}

func newFileStreamWriter(ws io.WriteSeeker) (*fileStreamWriter, error) {
	if _, err := ws.Seek(filePrefixSize, io.SeekStart); err != nil {
		return nil, err
	}

	return &fileStreamWriter{
		ws:  ws,
		bw:  bytespool.AcquireWriterSize(ws, int(units.MiB)),
		pos: filePrefixSize,
	}, nil
}

func (fw *fileStreamWriter) writeBlock(block indexBlock) error {
	header, payload := block.Bin(int64(fw.pos))
	if _, err := fw.bw.Write(payload); err != nil {
		return err
	}
	fw.hw.Write(header) // bytes.Buffer.Write never fails
	fw.pos += len(payload)
	return nil
}

func (fw *fileStreamWriter) finalize() (err error) {
	defer fw.release()
	if err = fw.bw.Flush(); err != nil {
		return
	}
	var regPos int64
	if regPos, err = fw.ws.Seek(0, io.SeekEnd); err != nil {
		return
	}
	if _, err = fw.bw.Write(fw.hw.Bytes()); err != nil {
		return
	}
	if err = fw.bw.Flush(); err != nil {
		return
	}
	prefix := binary.LittleEndian.AppendUint64(make([]byte, 0, filePrefixSize), uint64(regPos))
	prefix = binary.LittleEndian.AppendUint64(prefix, uint64(fw.hw.Len()))
	if _, err = fw.ws.Seek(0, io.SeekStart); err != nil {
		return
	}
	_, err = fw.ws.Write(prefix)
	return
}

func (fw *fileStreamWriter) release() {
	if fw.bw != nil {
		bytespool.ReleaseWriter(fw.bw)
		fw.bw = nil
	}
}
