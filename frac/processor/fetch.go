package processor

import (
	"fmt"

	"github.com/ozontech/seq-db/metric/stopwatch"
	"github.com/ozontech/seq-db/seq"
)

type fetchIndex interface {
	GetBlocksOffsets(uint32) uint64
	GetDocPos([]seq.ID) ([]seq.DocPos, error)
	ReadDocs(blockOffset uint64, docOffsets []uint64) ([][]byte, error)
}

func IndexFetch(ids []seq.ID, sw *stopwatch.Stopwatch, fetchIndex fetchIndex, res [][]byte) error {
	m := sw.Start("get_docs_pos")
	docsPos, err := fetchIndex.GetDocPos(ids)
	if err != nil {
		return err
	}
	blocks, offsets, index := seq.GroupDocsOffsets(docsPos)
	m.Stop()

	fmt.Printf("ids: %v\n", ids)
	fmt.Printf("blocks: %v\n", blocks)
	fmt.Printf("offsets: %v\n", offsets)

	m = sw.Start("read_doc")
	for i, docOffsets := range offsets {
		docs, err := fetchIndex.ReadDocs(fetchIndex.GetBlocksOffsets(blocks[i]), docOffsets)
		if err != nil {
			return err
		}
		fmt.Printf("docs: %v\n", docs)
		for src, dst := range index[i] {
			res[dst] = docs[src]
		}
	}
	m.Stop()
	return nil
}
