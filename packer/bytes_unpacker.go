// Package packer implements packaging of various types into bytes
package packer

import (
	"encoding/binary"
	"fmt"
)

type BytesUnpacker struct {
	buf []byte
}

func (u *BytesUnpacker) Len() int {
	return len(u.buf)
}

func NewBytesUnpacker(data []byte) *BytesUnpacker {
	return &BytesUnpacker{buf: data}
}

func (u *BytesUnpacker) GetVarint() (int64, error) {
	val, n := binary.Varint(u.buf)
	if n <= 0 {
		return 0, fmt.Errorf("varint returned invalid bytes read: %d", n)
	}
	u.buf = u.buf[n:]
	return val, nil
}

func (u *BytesUnpacker) GetUint32() uint32 {
	val := binary.LittleEndian.Uint32(u.buf)
	u.buf = u.buf[4:]
	return val
}

func (u *BytesUnpacker) GetBinary() []byte {
	l := u.GetUint32()
	val := u.buf[:l]
	u.buf = u.buf[l:]
	return val
}
