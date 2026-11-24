package blockedbloom

import (
	"fmt"
	"math"

	"github.com/cespare/xxhash/v2"
)

const BlockSize = bloomBlockBits / 8

const (
	// In a 64-byte block, the upper six bits of a position select the byte,
	// and the lower three bits select the bit within that byte
	bloomPositionBits = 9
	bloomBlockBits    = 1 << bloomPositionBits
)

const (
	maxBloomProbes = 16
	minBloomFPR    = 1e-6
)

type bloomLayout struct {
	blocks int
	probes int
}

func (l bloomLayout) blockOffset(hash uint64) int {
	return l.blockIndex(hash) * BlockSize
}

func (l bloomLayout) blockIndex(hash uint64) int {
	// NOTE(dkharms): Block selection uses integer division.
	// Consider `fastrange` if division becomes a hotspot in CPU profiles:
	// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
	return int(hash % uint64(l.blocks))
}

func (l bloomLayout) mayContainBlock(block *[BlockSize]byte, hash uint64) bool {
	positions := bloomPositions(hash >> 32)
	allSet := byte(1)

	for range l.probes {
		byteIndex, bitIndex := positions.next()
		allSet &= (block[byteIndex] >> bitIndex) & 1
	}

	return allSet != 0
}

type Filter struct {
	layout bloomLayout
	blocks [][BlockSize]byte
}

func New(keysCount uint64, fpr float64) (*Filter, error) {
	if !(minBloomFPR <= fpr && fpr < 1) {
		return nil, fmt.Errorf(
			"bloom false positive rate must be in [%g, 1), got %g",
			minBloomFPR, fpr,
		)
	}

	load, probes := estimateBloomParameters(fpr)
	blockCount := math.Ceil(float64(keysCount) / load)
	if blockCount > float64(maxBloomBlocks) {
		return nil, fmt.Errorf(
			"bloom filter is too large for %d keys at false positive rate %g",
			keysCount, fpr,
		)
	}

	groups := (int(blockCount) + blocksPerGroup - 1) / blocksPerGroup
	blocks := max(1, groups) * blocksPerGroup

	return newFilter(bloomLayout{blocks: blocks, probes: probes}), nil
}

func newFilter(layout bloomLayout) *Filter {
	return &Filter{
		layout: layout,
		blocks: make([][BlockSize]byte, layout.blocks),
	}
}

func (f *Filter) BlockCount() int {
	return len(f.blocks)
}

func (f *Filter) Block(index int) [BlockSize]byte {
	return f.blocks[index]
}

func (f *Filter) Add(hash uint64) {
	block := &f.blocks[f.layout.blockIndex(hash)]
	positions := bloomPositions(hash >> 32)

	for range f.layout.probes {
		byteIndex, bitIndex := positions.next()
		block[byteIndex] |= byte(1) << bitIndex
	}
}

func (f *Filter) MayContain(hash uint64) bool {
	block := &f.blocks[f.layout.blockIndex(hash)]
	return f.layout.mayContainBlock(block, hash)
}

func HashKey(key []byte) uint64 {
	return xxhash.Sum64(key)
}

type bloomPositions uint32

func (p *bloomPositions) next() (int, int) {
	position := *p >> (32 - bloomPositionBits)
	*p *= 0x9e3779b9
	return int(position / 8), int(position % 8)
}
