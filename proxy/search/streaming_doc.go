package search

import (
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/storage"
)

type StreamingDoc struct {
	ID     seq.ID
	Data   []byte
	Source uint64
}

func (d *StreamingDoc) Empty() bool {
	return len(d.Data) == 0
}

func (d *StreamingDoc) IDSource() seq.IDSource {
	return seq.IDSource{ID: d.ID, Source: d.Source}
}

func NewStreamingDoc(idSource seq.IDSource, data []byte) StreamingDoc {
	return StreamingDoc{
		ID:     idSource.ID,
		Source: idSource.Source,
		Data:   data,
	}
}

func unpackDoc(data []byte, source uint64) StreamingDoc {
	block := storage.DocBlock(data)
	mid := block.GetExt1()
	// micro to milli
	if mid > 1000000000000000 {
		mid = mid / 1000
	}
	doc := StreamingDoc{
		ID: seq.ID{
			MID: seq.MID(mid),
			RID: seq.RID(block.GetExt2()),
		},
		Source: source,
	}
	if block.Len() > 0 {
		doc.Data = block.Payload()
	}
	return doc
}
