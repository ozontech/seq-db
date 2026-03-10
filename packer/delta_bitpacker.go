package packer

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/ronanh/intcomp"
)

func CompressDeltaBitpackUint32(dst []byte, values []uint32, buf []uint32) []byte {
	buf = buf[:0]
	buf = append(buf, uint32(len(values)))

	var residual []uint32
	residual, buf = intcomp.CompressDeltaBinPackUint32(values, buf)

	// TODO comment here what residual is
	if len(residual) > 0 {
		buf = append(buf, residual...)
	}

	wordCount := uint32(len(buf))
	// TODO use memcpy
	dst = binary.LittleEndian.AppendUint32(dst, wordCount)
	for _, v := range buf {
		dst = binary.LittleEndian.AppendUint32(dst, v)
	}
	return dst
}

// TODO simplify
func DecompressDeltaBitpackUint32(data []byte, compressed, decompressed []uint32) ([]byte, []uint32, error) {
	if len(data) < 4 {
		return nil, nil, errors.New("lids block decode error: truncated sequence length header")
	}

	wordCount := binary.LittleEndian.Uint32(data[:4])
	data = data[4:]

	byteLen := int(wordCount) * 4
	if len(data) < byteLen {
		return nil, nil, errors.New("lids block decode error: truncated sequence payload")
	}

	u32Count := int(wordCount)
	if cap(compressed) < u32Count {
		compressed = make([]uint32, u32Count)
	} else {
		compressed = compressed[:u32Count]
	}
	// TODO manual copy
	for i := 0; i < u32Count; i++ {
		compressed[i] = binary.LittleEndian.Uint32(data[i*4 : i*4+4])
	}
	data = data[byteLen:]

	if len(compressed) == 0 {
		return data, nil, nil
	}
	total := int(compressed[0])
	if total == 0 {
		return data, nil, nil
	}

	payload := compressed[1:]

	decompressed = decompressed[:0]
	switch {
	case total < intcomp.BitPackingBlockSize32:
		if len(payload) < total {
			return nil, nil, errors.New("lids block decode error: residual payload truncated")
		}
		decompressed = append(decompressed, payload[:total]...)
	default:
		remaining, out := intcomp.UncompressDeltaBinPackUint32(payload, decompressed)
		decompressed = out
		if len(remaining) > 0 {
			decompressed = append(decompressed, remaining...)
		}
		if len(decompressed) < total {
			return nil, nil, errors.New("lids block decode error: decompressed length mismatch")
		}
		decompressed = decompressed[:total]
	}

	return data, decompressed[:total], nil
}

func CompressDeltaBitpackUint64(dst []byte, values []uint64, buf []uint64) []byte {
	buf = buf[:0]
	total := len(values)
	buf = append(buf, uint64(total))

	var residual []uint64
	residual, buf = intcomp.CompressDeltaBinPackUint64(values, buf)
	if len(residual) > 0 {
		buf = append(buf, residual...)
	}

	wordCount := uint32(len(buf))
	// TODO use memcpy
	dst = binary.LittleEndian.AppendUint32(dst, wordCount)
	for _, v := range buf {
		dst = binary.LittleEndian.AppendUint64(dst, v)
	}
	return dst
}

// TODO simplify
func DecompressDeltaBitpackUint64(data []byte, compressed, values []uint64) ([]byte, []uint64, error) {
	if len(data) < 4 {
		return nil, nil, errors.New("mids block decode error: truncated sequence length header")
	}

	wordCount := binary.LittleEndian.Uint32(data[:4])
	data = data[4:]

	byteLen := int(wordCount) * 8
	if len(data) < byteLen {
		return nil, nil, errors.New("mids block decode error: truncated sequence payload")
	}

	u64Count := int(wordCount)
	if cap(compressed) < u64Count {
		compressed = make([]uint64, u64Count)
	} else {
		compressed = compressed[:u64Count]
	}
	// TODO we reinterpret []byte as []uint64 and them copy to compressed
	// maybe we could get rid of compressed?
	src := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(data[:byteLen]))), u64Count)
	copy(compressed, src)
	data = data[byteLen:]

	if len(compressed) == 0 {
		return data, nil, nil
	}
	total := int(compressed[0])
	if total == 0 {
		return data, nil, nil
	}

	payload := compressed[1:]

	values = values[:0]
	var decompressed []uint64
	switch {
	case total < intcomp.BitPackingBlockSize64:
		if len(payload) < total {
			return nil, nil, errors.New("mids block decode error: residual payload truncated")
		}
		decompressed = append(values[:0], payload[:total]...)
	default:
		remaining, out := intcomp.UncompressDeltaBinPackUint64(payload, values[:0])
		decompressed = out
		if len(remaining) > 0 {
			decompressed = append(decompressed, remaining...)
		}
		if len(decompressed) < total {
			return nil, nil, errors.New("mids block decode error: decompressed length mismatch")
		}
		decompressed = decompressed[:total]
	}

	return data, decompressed[:total], nil
}
