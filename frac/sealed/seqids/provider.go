package seqids

import (
	"github.com/ozontech/seq-db/cache"
	"github.com/ozontech/seq-db/config"
	"github.com/ozontech/seq-db/node"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

type Provider struct {
	table    *Table
	loader   Loader
	midCache *unpackCache
	ridCache *unpackCache
	posCache *unpackCache
}

func NewProvider(
	indexReader *storage.IndexReader,
	cacheMIDs cache.Wrapper[[]byte],
	cacheRIDs cache.Wrapper[BlockRIDs],
	cacheParams cache.Wrapper[BlockParams],
	table *Table,
	fracVersion config.BinaryDataVersion,
) *Provider {
	return &Provider{
		table: table,
		loader: Loader{
			reader:      indexReader,
			table:       table,
			mids:        cacheMIDs,
			rids:        cacheRIDs,
			params:      cacheParams,
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

func (p *Provider) MIDs(lids []node.LID, out []seq.MID) ([]seq.MID, error) {
	for _, lid := range lids {
		rawLid := lid.Unpack()
		blockIdx := p.table.GetIDBlockIndexByLID(rawLid)
		if p.midCache.blockIndex != int(blockIdx) {
			if err := p.fillMIDs(blockIdx, p.midCache); err != nil {
				return nil, err
			}
		}
		out = append(out, seq.MID(p.midCache.GetValByLID(rawLid)))
	}
	return out, nil
}

func (p *Provider) fillMIDs(blockIndex uint32, dst *unpackCache) error {
	if dst.blockIndex != int(blockIndex) {
		block, err := p.loader.GetMIDsBlock(blockIndex, dst)
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

func (p *Provider) RIDs(lids []node.LID, out []seq.RID) ([]seq.RID, error) {
	for _, lid := range lids {
		rawLid := lid.Unpack()
		blockIndex := p.table.GetIDBlockIndexByLID(rawLid)
		if p.ridCache.blockIndex != int(blockIndex) {
			if err := p.fillRIDs(blockIndex, p.ridCache); err != nil {
				return nil, err
			}
		}

		out = append(out, seq.RID(p.ridCache.GetValByLID(rawLid)))
	}

	return out, nil
}

func (p *Provider) fillRIDs(blockIndex uint32, dst *unpackCache) error {
	if dst.blockIndex != int(blockIndex) {
		block, err := p.loader.GetRIDsBlock(blockIndex)
		if err != nil {
			return err
		}
		dst.blockIndex = int(blockIndex)
		dst.startLID = p.loader.table.BlockStartLID(blockIndex)
		// we have to copy `block.Values` because we store them in `cache.Cache[BlockRIDs]`,
		// but `dst *unpackCache` might put its `values` in sync.Pool on `release()`, and they
		// will be reused and corrupted
		dst.values = append(dst.values[:0], block.Values...)
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
