package util

type Bitmask struct {
	size int
	bin  []byte
}

const bitsInByte = 8

func NewBitmask(size int) Bitmask {
	return Bitmask{
		size: size,
		bin:  make([]byte, (size+bitsInByte-1)/bitsInByte),
	}
}

func (b *Bitmask) GetSize() int {
	return b.size
}

func (b *Bitmask) Get(pos int) bool {
	byteIndex := pos / bitsInByte
	bitIndex := pos % bitsInByte
	return (b.bin[byteIndex] & (1 << bitIndex)) > 0
}

func (b *Bitmask) Set(pos int, state bool) {
	byteIndex := pos / bitsInByte
	bitIndex := pos % bitsInByte
	mask := byte(1 << bitIndex)
	if state {
		b.bin[byteIndex] |= mask
	} else {
		b.bin[byteIndex] &= ^mask
	}
}

func (b *Bitmask) HasBitsIn(left, right int) bool {
	const allOnes = byte(0xFF)

	leftIndex := left / bitsInByte
	rightIndex := right / bitsInByte

	leftBitIndex := left % bitsInByte
	rightBitIndex := right%bitsInByte + 1

	leftMask := allOnes << leftBitIndex
	rightMask := allOnes >> (bitsInByte - rightBitIndex)

	if leftIndex == rightIndex {
		return b.bin[leftIndex]&leftMask&rightMask > 0
	}

	if b.bin[leftIndex]&leftMask > 0 {
		return true
	}

	if b.bin[rightIndex]&rightMask > 0 {
		return true
	}

	for i := leftIndex + 1; i < rightIndex; i++ {
		if b.bin[i] > 0 {
			return true
		}
	}

	return false
}

func (b *Bitmask) GetBitmaskBinary() []byte {
	return b.bin
}

func LoadBitmask(size int, data []byte) Bitmask {
	b := NewBitmask(size)
	b.bin = append(b.bin[:0], data[:len(b.bin)]...)
	return b
}
