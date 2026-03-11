package packer

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/ronanh/intcomp"
)

// CompressDeltaBitpackUint32 works on top of intcomp library. intcomp can only compress slices which are multiple of 128, but
// this function supports slices of any length. Residual part is always less than 128 numbers and is not delta encoded,
// since we know the number of blocks with length non-multiple of 128 is very low.
func CompressDeltaBitpackUint32(dst []byte, values, buf []uint32) []byte {
	buf = buf[:0]
	buf = append(buf, uint32(len(values)))

	var residual []uint32
	residual, buf = intcomp.CompressDeltaBinPackUint32(values, buf)

	if len(residual) > 0 {
		// append residual as is. always less than 128 values
		buf = append(buf, residual...)
	}

	{
		// TODO use memcpy
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(buf)))
		for _, v := range buf {
			dst = binary.LittleEndian.AppendUint32(dst, v)
		}
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

	// TODO if little endian => reinterpret cast data []byte as []uint32
	// otherwise, allocate slice
	// remove compressed param
	{
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
	}

	count := int(compressed[0])
	if count == 0 {
		return data, nil, nil
	}

	compressed = compressed[1:]

	decompressed = decompressed[:0]
	switch {
	case count < intcomp.BitPackingBlockSize32:
		if len(compressed) < count {
			return nil, nil, errors.New("lids block decode error: residual payload truncated")
		}
		decompressed = append(decompressed, compressed[:count]...)
	default:
		remaining, out := intcomp.UncompressDeltaBinPackUint32(compressed, decompressed)
		decompressed = out
		if len(remaining) > 0 {
			decompressed = append(decompressed, remaining...)
		}
		if len(decompressed) < count {
			return nil, nil, errors.New("lids block decode error: decompressed length mismatch")
		}
		decompressed = decompressed[:count]
	}

	return data, decompressed[:count], nil
}

// CompressDeltaBitpackUint64 works on top of intcomp library. intcomp can only compress uint64 slices which are multiple of 256, but
// this function supports slices of any length. Residual part is always less than 256 uint64 numbers and is not delta encoded,
// since we know the number of blocks with length non-multiple of 256 is very low.
func CompressDeltaBitpackUint64(dst []byte, values, buf []uint64) []byte {
	buf = buf[:0]
	total := len(values)
	buf = append(buf, uint64(total))

	var residual []uint64
	residual, buf = intcomp.CompressDeltaBinPackUint64(values, buf)
	if len(residual) > 0 {
		// append residual as is. always less than 256 values
		buf = append(buf, residual...)
	}

	{
		// TODO use memcpy
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(buf)))
		for _, v := range buf {
			dst = binary.LittleEndian.AppendUint64(dst, v)
		}
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

	// TODO if little endian => reinterpret cast data []byte as []uint64
	// otherwise, allocate slice
	// remove compressed param
	{
		u64Count := int(wordCount)
		if cap(compressed) < u64Count {
			compressed = make([]uint64, u64Count)
		} else {
			compressed = compressed[:u64Count]
		}
		src := unsafe.Slice((*uint64)(unsafe.Pointer(unsafe.SliceData(data[:byteLen]))), u64Count)
		copy(compressed, src)
		data = data[byteLen:]

		if len(compressed) == 0 {
			return data, nil, nil
		}
	}

	count := int(compressed[0])
	if count == 0 {
		return data, nil, nil
	}

	compressed = compressed[1:]
	var decompressed []uint64
	switch {
	case count < intcomp.BitPackingBlockSize64:
		if len(compressed) < count {
			return nil, nil, errors.New("mids block decode error: residual payload truncated")
		}
		decompressed = append([]uint64{}, compressed[:count]...)
	default:
		values = values[:0]
		remaining, out := intcomp.UncompressDeltaBinPackUint64(compressed, values[:0])
		decompressed = out
		if len(remaining) > 0 {
			decompressed = append(decompressed, remaining...)
		}
		if len(decompressed) < count {
			return nil, nil, errors.New("mids block decode error: decompressed length mismatch")
		}
		decompressed = decompressed[:count]
	}

	return data, decompressed[:count], nil
}
