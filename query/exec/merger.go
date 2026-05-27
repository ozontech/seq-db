package exec

import (
	"cmp"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/query"
)

type Merger struct {
	left, right query.RecordProducer

	curLeft, curRight *query.Record

	colIdx   int
	field    string
	dataType query.DataType
	order    Order
	less     func(any, any) int

	done bool
}

func NewMerger(
	left query.RecordProducer,
	right query.RecordProducer,
	colIdx int,
	field string,
	dataType query.DataType,
	order Order,
) *Merger {
	less := createLessFunc()

	return &Merger{
		left:     left,
		right:    right,
		colIdx:   colIdx,
		field:    field,
		dataType: dataType,
		order:    order,
		less:     less,
		curLeft:  nil,
		curRight: nil,
		done:     false,
	}
}

func (m *Merger) Next() (*query.Record, *query.Metadata) {
	if m.done {
		return nil, nil
	}

	if m.curLeft == nil {
		m.curLeft, _ = m.left.Next()
	}
	if m.curRight == nil {
		m.curRight, _ = m.right.Next()
	}

	if m.curLeft == nil && m.curRight == nil {
		m.done = true
		return nil, nil
	}

	if m.curLeft == nil {
		r := m.curRight
		m.curRight, _ = m.right.Next()
		return r, nil
	}

	if m.curRight == nil {
		r := m.curLeft
		m.curLeft, _ = m.left.Next()
		return r, nil
	}

	leftVal := m.extractValue(m.curLeft)
	rightVal := m.extractValue(m.curRight)

	compared := m.less(leftVal, rightVal)
	chooseLeft := compared <= 0
	if m.order == OrderDesc {
		chooseLeft = compared >= 0
	}

	if chooseLeft {
		r := m.curLeft
		m.curLeft, _ = m.left.Next()
		return r, nil
	}

	r := m.curRight
	m.curRight, _ = m.right.Next()
	return r, nil
}

func (m *Merger) extractValue(r *query.Record) any {
	val := r.Vals[m.colIdx]
	decoded := val.Decoded()

	switch m.dataType {
	case query.DataTypeDocument:
		if m.field == "" {
			return decoded
		}
		return decoded.(*insaneJSON.Root).Dig(m.field).AsString()
	case query.DataTypeString:
		return decoded.(string)
	case query.DataTypeUint32:
		return decoded.(uint32)
	case query.DataTypeUint64:
		return decoded.(uint64)
	case query.DataTypeInt32:
		return decoded.(int32)
	case query.DataTypeInt64:
		return decoded.(int64)
	case query.DataTypeFloat64:
		return decoded.(float64)
	default:
		return ""
	}
}

func createLessFunc() func(any, any) int {
	// TODO: make use of m.dataType (???)
	return func(a, b any) int {
		switch v := a.(type) {
		case uint32:
			return cmp.Compare(v, b.(uint32))
		case uint64:
			return cmp.Compare(v, b.(uint64))
		case int32:
			return cmp.Compare(v, b.(int32))
		case int64:
			return cmp.Compare(v, b.(int64))
		case float64:
			return cmp.Compare(v, b.(float64))
		case string:
			return cmp.Compare(v, b.(string))
		default:
			return 0 // TODO: ???
		}
	}
}

func NewNMergedProducers(
	producers []query.RecordProducer,
	colIdx int,
	field string,
	dataType query.DataType,
	order Order,
) query.RecordProducer {
	if len(producers) == 0 {
		return &emptyRecordProducer{}
	}

	if len(producers) == 1 {
		return NewMerger(producers[0], &emptyRecordProducer{}, colIdx, field, dataType, order)
	}

	merged := NewMerger(producers[0], producers[1], colIdx, field, dataType, order)
	for _, p := range producers[2:] {
		merged = NewMerger(merged, p, colIdx, field, dataType, order)
	}
	return merged
}

type emptyRecordProducer struct{}

func (e *emptyRecordProducer) Next() (*query.Record, *query.Metadata) {
	return nil, nil
}
