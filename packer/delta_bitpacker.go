package packer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"github.com/ronanh/intcomp"
)

const (
	sizeOfUint32 = int(unsafe.Sizeof(uint32(0)))
)

var uint32Pool = sync.Pool{
	New: func() any {
		return make([]uint32, 0, 16*1024)
	},
}

// CompressDeltaBitpackUint32 works on top of intcomp library. intcomp can only compress slices which are multiple of 128, but
// this function supports slices of any length. Residual part is always less than 128 numbers and is not delta encoded,
// since we know the number of blocks with length non-multiple of 128 is very low.
func CompressDeltaBitpackUint32(dst []byte, values, buf []uint32) []byte {
	buf = buf[:0]
	buf = append(buf, uint32(len(values)))

	var residual []uint32
	residual, buf = intcomp.CompressDeltaBinPackUint32(values, buf)

	if len(residual) > 0 {
		// append residual as is. always less than 128 values and most blocks will never enter this branch
		buf = append(buf, residual...)
	}

	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(buf)))
	for _, v := range buf {
		dst = binary.LittleEndian.AppendUint32(dst, v)
	}

	return dst
}

// CompressDeltaBitpackUint16 uses a temporary buffer to copy and cast values from uint16 to uint32 so it's a bit slower than CompressDeltaBitpackUint32.
func CompressDeltaBitpackUint16(dst []byte, values []uint16, buf []uint32) []byte {
	uint32Values, _ := uint32Pool.Get().([]uint32)
	uint32Values = uint32Values[:0]

	for _, i := range values {
		uint32Values = append(uint32Values, uint32(i))
	}
	dst = CompressDeltaBitpackUint32(dst, uint32Values, buf)
	uint32Pool.Put(uint32Values)
	return dst
}

func DecompressDeltaBitpackUint32(data []byte, buf, compressed []uint32) ([]byte, []uint32, error) {
	if len(data) < sizeOfUint32 {
		return nil, nil, fmt.Errorf("not enough data. slice len %d", len(data))
	}

	uintsCount := binary.LittleEndian.Uint32(data[:4])
	data = data[sizeOfUint32:]

	byteLen := int(uintsCount) * sizeOfUint32
	if len(data) < byteLen {
		return nil, nil, fmt.Errorf("not enough data: expected %d bytes, got %d", byteLen, len(data))
	}

	compressed = copyAsUints32(data[:byteLen], compressed)
	data = data[byteLen:]

	count := int(compressed[0])
	if count == 0 {
		return data, nil, nil
	}

	compressed = compressed[1:]
	buf = buf[:0]
	switch {
	case count < intcomp.BitPackingBlockSize32:
		if len(compressed) < count {
			return nil, nil, errors.New("not enough data")
		}
		buf = append(buf, compressed[:count]...)
	default:
		var residual []uint32
		residual, buf = intcomp.UncompressDeltaBinPackUint32(compressed, buf)

		if len(residual) > 0 {
			buf = append(buf, residual...)
		}
		if len(buf) < count {
			return nil, nil, errors.New("length mismatch")
		}
		buf = buf[:count]
	}

	return data, buf, nil
}

// DecompressDeltaBitpackUint16 works on top of DecompressDeltaBitpackUint32 so it's a bit slower
func DecompressDeltaBitpackUint16(data []byte, buf []uint16, compressed []uint32) ([]byte, []uint16, error) {
	uint32Values, _ := uint32Pool.Get().([]uint32)
	uint32Values = uint32Values[:0]

	var (
		values []uint32
		err    error
	)

	data, values, err = DecompressDeltaBitpackUint32(data, uint32Values, compressed)

	for _, i := range values {
		buf = append(buf, uint16(i))
	}
	uint32Pool.Put(uint32Values)

	return data, buf, err
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
		// append residual as is. always less than 256 values and most blocks will never enter this branch
		buf = append(buf, residual...)
	}

	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(buf)))
	for _, v := range buf {
		dst = binary.LittleEndian.AppendUint64(dst, v)
	}

	return dst
}

func DecompressDeltaBitpackUint64(data []byte, buf, compressed []uint64) ([]byte, []uint64, error) {
	if len(data) < 4 {
		return nil, nil, fmt.Errorf("not enough data. slice len %d", len(data))
	}

	uintsCount := binary.LittleEndian.Uint32(data[:4])
	data = data[4:]

	byteLen := int(uintsCount) * 8
	if len(data) < byteLen {
		return nil, nil, fmt.Errorf("not enough data: expected %d bytes, got %d", byteLen, len(data))
	}

	compressed = copyAsUints64(data[:byteLen], compressed)
	data = data[byteLen:]

	count := int(compressed[0])
	if count == 0 {
		return data, nil, nil
	}

	compressed = compressed[1:]
	buf = buf[:0]
	switch {
	case count < intcomp.BitPackingBlockSize64:
		if len(compressed) < count {
			return nil, nil, errors.New("not enough data")
		}
		buf = append(buf, compressed[:count]...)
	default:
		var residual []uint64
		residual, buf = intcomp.UncompressDeltaBinPackUint64(compressed, buf[:0])
		if len(residual) > 0 {
			buf = append(buf, residual...)
		}
		if len(buf) < count {
			return nil, nil, errors.New("length mismatch")
		}
		buf = buf[:count]
	}

	return data, buf, nil
}

func copyAsUints32(src []byte, dst []uint32) []uint32 {
	dst = dst[:0]
	for len(src) != 0 {
		dst = append(dst, binary.LittleEndian.Uint32(src))
		src = src[sizeOfUint32:]
	}
	return dst
}

func copyAsUints64(src []byte, dst []uint64) []uint64 {
	dst = dst[:0]
	for len(src) != 0 {
		dst = append(dst, binary.LittleEndian.Uint64(src))
		src = src[8:]
	}
	return dst
}
