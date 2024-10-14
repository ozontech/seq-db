package search

import (
	"github.com/ozontech/seq-db/disk"
	"github.com/ozontech/seq-db/seq"
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
	block := disk.DocBlock(data)
	doc := StreamingDoc{
		ID: seq.ID{
			MID: seq.MID(block.GetExt1()),
			RID: seq.RID(block.GetExt2()),
		},
		Source: source,
	}
	if block.Len() > 0 {
		doc.Data = block.Payload()
	}
	return doc
}
