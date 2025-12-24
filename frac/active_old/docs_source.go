package active_old

import (
	"iter"

	"github.com/ozontech/seq-db/consts"
	"github.com/ozontech/seq-db/frac/sealed/sealing"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

var _ sealing.DocsSource = (*DocsSource)(nil)

type DocsSource struct {
	src           sealing.Source
	blocksOffsets []uint64
	docsReader    *storage.DocsReader
	lastErr       error
}

func NewDocsSource(src sealing.Source, blocksOffsets []uint64, docsReader *storage.DocsReader) *DocsSource {
	return &DocsSource{
		src:           src,
		blocksOffsets: blocksOffsets,
		docsReader:    docsReader,
	}
}

// Docs returns an iterator for documents with their IDs.
// Handles duplicate IDs (for nested indexes).
func (ds *DocsSource) Docs() iter.Seq2[seq.ID, []byte] {
	ds.lastErr = nil
	return func(yield func(seq.ID, []byte) bool) {
		var (
			prev   seq.ID
			curDoc []byte
		)

		// iterate through ID and position blocks
		for ids, pos := range ds.src.IDsBlocks(consts.IDsPerBlock) {
			for i, id := range ids {
				if id == systemSeqID {
					curDoc = nil // reserved system document (no payload)
				} else if id != prev {
					// if ID changed, read new document
					if curDoc, ds.lastErr = ds.doc(pos[i]); ds.lastErr != nil {
						return
					}
				}
				prev = id
				if !yield(id, curDoc) {
					return
				}
			}
		}
	}
}

// doc reads a document from storage by its position.
func (ds *DocsSource) doc(pos seq.DocPos) ([]byte, error) {
	blockIndex, docOffset := pos.Unpack()
	blockOffset := ds.blocksOffsets[blockIndex]

	var doc []byte
	err := ds.docsReader.ReadDocsFunc(blockOffset, []uint64{docOffset}, func(b []byte) error {
		doc = b
		return nil
	})
	if err != nil {
		return nil, err
	}
	return doc, nil
}

func (ds *DocsSource) LastError() error {
	if ds.lastErr != nil {
		return ds.lastErr
	}
	return ds.src.LastError()
}
