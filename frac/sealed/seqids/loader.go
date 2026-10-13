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
	MinBlockIDs     []seq.ID // From max to min.
	IDsTotal        uint32   // Actually, this is LIDs count.
	StartBlockIndex uint32
}

func (Table) GetIDBlockIndexByLID(lid uint32) uint32 {
	return lid / uint32(consts.IDsPerBlock)
}

func (Table) BlockStartLID(blockIndex uint32) uint32 {
	return blockIndex * uint32(consts.IDsPerBlock)
}

type Loader struct {
	fracVersion config.BinaryDataVersion
	reader      *storage.IndexReader
	table       *Table

	mids   cache.Cache[[]byte]
	rids   cache.Cache[BlockRIDs]
	params cache.Cache[BlockParams]
}

type midsLoader Loader

func (ml *midsLoader) midBlockIndex(index uint32) uint32 {
	return ml.table.StartBlockIndex + index*3
}

func (ml *midsLoader) Load(index uint32) ([]byte, int, error) {
	data, _, err := ml.reader.ReadIndexBlock(ml.midBlockIndex(index), nil)
	return data, cap(data), err
}

func (l *Loader) GetMIDsBlock(index uint32, unpackCache *unpackCache) (BlockMIDs, error) {
	data, err := l.mids.Get(index, (*midsLoader)(l))
	if err == nil && len(data) == 0 {
		return BlockMIDs{}, errors.New("empty block")
	}

	if err != nil {
		return BlockMIDs{}, err
	}

	block := BlockMIDs{Values: unpackCache.values[:0]}
	if err := block.Unpack(data, l.fracVersion, unpackCache); err != nil {
		return BlockMIDs{}, err
	}

	return block, nil
}

type ridsLoader Loader

func (rl *ridsLoader) ridBlockIndex(index uint32) uint32 {
	return rl.table.StartBlockIndex + index*3 + 1
}

func (rl *ridsLoader) Load(index uint32) (BlockRIDs, int, error) {
	l := (*Loader)(rl)

	data, _, err := l.reader.ReadIndexBlock(rl.ridBlockIndex(index), nil)
	if err != nil {
		return BlockRIDs{}, 0, err
	}

	block := BlockRIDs{
		fracVersion: l.fracVersion,
		Values:      make([]uint64, 0, consts.IDsPerBlock),
	}

	err = block.Unpack(data)
	if err != nil {
		return BlockRIDs{}, 0, err
	}

	if len(block.Values) == 0 {
		return BlockRIDs{}, 0, errors.New("empty block")
	}

	const ui64 = int(unsafe.Sizeof(uint64(0)))
	return block, cap(block.Values) * ui64, err
}

func (l *Loader) GetRIDsBlock(index uint32) (BlockRIDs, error) {
	return l.rids.Get(index, (*ridsLoader)(l))
}

type paramsLoader Loader

func (pl *paramsLoader) paramsBlockIndex(index uint32) uint32 {
	return pl.table.StartBlockIndex + index*3 + 2
}

func (pl *paramsLoader) Load(index uint32) (BlockParams, int, error) {
	l := (*Loader)(pl)

	data, _, err := l.reader.ReadIndexBlock(pl.paramsBlockIndex(index), nil)
	if err != nil {
		return BlockParams{}, 0, err
	}

	block := BlockParams{Values: make([]uint64, 0, consts.IDsPerBlock)}
	if err := block.Unpack(data); err != nil {
		return BlockParams{}, 0, err
	}

	if len(block.Values) == 0 {
		return BlockParams{}, 0, errors.New("empty block")
	}

	const ui64 = int(unsafe.Sizeof(uint64(0)))
	return block, cap(block.Values) * ui64, nil
}

func (l *Loader) GetParamsBlock(index uint32) (BlockParams, error) {
	return l.params.Get(index, (*paramsLoader)(l))
}
