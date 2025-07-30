package seqids

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/conf"
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/seq"
)

type Provider struct {
	table    *Table
	loader   Loader
	midCache *unpackCache
	ridCache *unpackCache
	posCache *unpackCache
}

func NewProvider(
	indexReader *disk.IndexReader,
	cacheMIDs *cache.Cache[[]byte],
	cacheRIDs *cache.Cache[[]byte],
	cacheParams *cache.Cache[BlockParams],
	table *Table,
	fracVersion conf.BinaryDataVersion,
) *Provider {
	return &Provider{
		table: table,
		loader: Loader{
			reader:      indexReader,
			table:       table,
			cacheMIDs:   cacheMIDs,
			cacheRIDs:   cacheRIDs,
			cacheParams: cacheParams,
			fracVersion: fracVersion,
		},
		midCache: NewCache(),
		ridCache: NewCache(),
		posCache: NewCache(),
	}
}

func (p *Provider) Release() {
	p.midCache.Release()
	p.ridCache.Release()
	p.posCache.Release()
}

func (p *Provider) MID(lid seq.LID) (seq.MID, error) {
	blockIndex := p.table.GetIDBlockIndexByLID(uint32(lid))
	if err := p.fillMIDs(blockIndex, p.midCache); err != nil {
		return 0, err
	}
	return seq.MID(p.midCache.GetValByLID(uint32(lid))), nil
}

func (p *Provider) fillMIDs(blockIndex uint32, dst *unpackCache) error {
	if dst.blockIndex != int(blockIndex) {
		block, err := p.loader.GetMIDsBlock(blockIndex, dst.values[:0])
		if err != nil {
			return err
		}
		dst.blockIndex = int(blockIndex)
		dst.startLID = p.loader.table.BlockStartLID(blockIndex)
		dst.values = block.Values
	}
	return nil
}

func (p *Provider) RID(lid seq.LID) (seq.RID, error) {
	blockIndex := p.table.GetIDBlockIndexByLID(uint32(lid))
	if err := p.fillRIDs(blockIndex, p.ridCache); err != nil {
		return 0, err
	}
	return seq.RID(p.ridCache.GetValByLID(uint32(lid))), nil
}

func (p *Provider) fillRIDs(blockIndex uint32, dst *unpackCache) error {
	if dst.blockIndex != int(blockIndex) {
		block, err := p.loader.GetRIDsBlock(blockIndex, dst.values[:0])
		if err != nil {
			return err
		}
		dst.blockIndex = int(blockIndex)
		dst.startLID = p.loader.table.BlockStartLID(blockIndex)
		dst.values = block.Values
	}
	return nil
}

func (p *Provider) DocPos(lid seq.LID) (seq.DocPos, error) {
	blockIndex := p.table.GetIDBlockIndexByLID(uint32(lid))
	if err := p.fillParams(blockIndex, p.posCache); err != nil {
		return 0, err
	}
	return seq.DocPos(p.posCache.GetValByLID(uint32(lid))), nil
}

func (p *Provider) fillParams(blockIndex uint32, dst *unpackCache) error {
	if dst.blockIndex != int(blockIndex) {
		block, err := p.loader.GetParamsBlock(blockIndex)
		if err != nil {
			return err
		}
		dst.blockIndex = int(blockIndex)
		dst.startLID = p.loader.table.BlockStartLID(blockIndex)
		// we have to copy `block.Values` because we store them in `cache.Cache[BlockParams]`,
		// but `dst *unpackCache` might put its `values` in sync.Pool on `release()`, and they
		// will be reused and corrupted
		dst.values = append(dst.values[:0], block.Values...)
	}
	return nil
}
