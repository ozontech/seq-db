// Package packer implements packaging of various types into bytes
package packer

import (
	"encoding/binary"
)

type BytesPacker struct {
	buf  []byte
	Data []byte
}

func NewBytesPacker(block []byte) *BytesPacker {
	return &BytesPacker{
		buf:  make([]byte, 10),
		Data: block,
	}
}

func (p *BytesPacker) PutVarint(num int64) {
	n := binary.PutVarint(p.buf, num)
	p.Data = append(p.Data, p.buf[:n]...)
}

func (p *BytesPacker) PutUint32(num uint32) {
	binary.LittleEndian.PutUint32(p.buf, num)
	p.Data = append(p.Data, p.buf[:4]...)
}

func (p *BytesPacker) PutUint64(num uint64) {
	binary.LittleEndian.PutUint64(p.buf, num)
	p.Data = append(p.Data, p.buf[:8]...)
}

func (p *BytesPacker) PutBytes(b []byte) {
	p.Data = append(p.Data, b...)
}

func (p *BytesPacker) PutStringWithSize(s string) {
	p.PutUint32(uint32(len(s)))
	p.Data = append(p.Data, s...)
}
