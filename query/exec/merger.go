package exec

import "github.com/ozontech/seq-db/query"

// nolint:unused // TODO:
type Merger struct {
	left  query.RecordProducer
	right query.RecordProducer

	curLeft  *query.Record
	curRight *query.Record

	colIdx int
}

func NewMerger(
	left query.RecordProducer,
	right query.RecordProducer,
) *Merger {
	return &Merger{
		left:  left,
		right: right,
	}
}

func (m *Merger) Next() (*query.Record, *query.Metadata) {
	return nil, nil
}
