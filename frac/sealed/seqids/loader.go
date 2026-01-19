package seqids

import (
	"errors"
	"unsafe"

	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

type Table struct {
	MinBlockIDs     []seq.ID // from max to min
	IDBlocksTotal   uint32
	IDsTotal        uint32
	StartBlockIndex uint32
}

func (Table) GetIDBlockIndexByLID(lid uint32) uint32 {
	return lid / uint32(consts.IDsPerBlock)
}

func (Table) BlockStartLID(blockIndex uint32) uint32 {
	return blockIndex * uint32(consts.IDsPerBlock)
}

type Loader struct {
	reader      *storage.IndexReader
	table       *Table
	cacheMIDs   *cache.Cache[[]byte]
	cacheRIDs   *cache.Cache[BlockRIDs]
	cacheParams *cache.Cache[BlockParams]
	fracVersion config.BinaryDataVersion
}

func (l *Loader) GetMIDsBlock(index uint32, buf []uint64) (BlockMIDs, error) {
	// load binary from index
	data, err := l.cacheMIDs.GetWithError(index, func() ([]byte, int, error) {
		data, _, err := l.reader.ReadIndexBlock(l.midBlockIndex(index), nil)
		return data, cap(data), err
	})
	// check errors
	if err == nil && len(data) == 0 {
		err = errors.New("empty block")
	}
	if err != nil {
		return BlockMIDs{}, err
	}
	// unpack
	block := BlockMIDs{Values: buf}
	if err := block.Unpack(data, l.fracVersion); err != nil {
		return BlockMIDs{}, err
	}
	return block, nil
}

func (l *Loader) GetRIDsBlock(index uint32, buf []uint64) (BlockRIDs, error) {
	block, err := l.cacheRIDs.GetWithError(index, func() (BlockRIDs, int, error) {
		data, _, err := l.reader.ReadIndexBlock(l.ridBlockIndex(index), nil)
		if err != nil {
			return BlockRIDs{}, 0, err
		}

		ui64 := int(unsafe.Sizeof(uint64(0)))

		length := len(data) / ui64
		cached := make([]uint64, length)

		block := BlockRIDs{
			fracVersion: l.fracVersion,
			Values:      cached,
		}

		err = block.Unpack(data)
		return block, cap(block.Values) * ui64, err
	})

	if err != nil {
		return BlockRIDs{}, err
	}

	if len(block.Values) == 0 {
		return BlockRIDs{}, errors.New("empty block")
	}

	for i := range len(block.Values) {
		buf = append(buf, block.Values[i])
	}

	return BlockRIDs{
		fracVersion: l.fracVersion,
		Values:      buf,
	}, nil
}

func (l *Loader) GetParamsBlock(index uint32) (BlockParams, error) {
	// load binary from index
	block, err := l.cacheParams.GetWithError(index, func() (BlockParams, int, error) {
		data, _, err := l.reader.ReadIndexBlock(l.paramsBlockIndex(index), nil)
		if err != nil {
			return BlockParams{}, 0, err
		}
		// unpack
		block := BlockParams{Values: make([]uint64, 0, consts.IDsPerBlock)}
		if err := block.Unpack(data); err != nil {
			return BlockParams{}, 0, err
		}
		if len(block.Values) == 0 {
			return BlockParams{}, 0, errors.New("empty block")
		}
		return block, cap(block.Values) * 8, nil
	})
	// check errors
	if err != nil {
		return BlockParams{}, err
	}
	return block, nil
}

// blocks are stored as triplets on disk, (MID + RID + Pos), check docs/format-index-file.go
func (l *Loader) midBlockIndex(index uint32) uint32 {
	return l.table.StartBlockIndex + index*3
}

func (l *Loader) ridBlockIndex(index uint32) uint32 {
	return l.table.StartBlockIndex + index*3 + 1
}

func (l *Loader) paramsBlockIndex(index uint32) uint32 {
	return l.table.StartBlockIndex + index*3 + 2
}
