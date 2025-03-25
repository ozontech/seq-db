package active2

import (
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/seq"
)

type fetchIndex struct {
	index      *Index
	docsReader *disk.DocsReader
}

func (si *fetchIndex) GetBlocksOffsets(blockIndex uint32) uint64 {
	return si.index.blocksOffsets[blockIndex]
}

func (si *fetchIndex) GetDocPos(ids []seq.ID) []seq.DocPos {
	docsPos := make([]seq.DocPos, len(ids))
	for i, id := range ids {
		docsPos[i] = si.index.positions[id]
	}
	return docsPos
}

func (si *fetchIndex) ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error) {
	return si.docsReader.ReadDocs(blockOffset, docOffsets)
}
