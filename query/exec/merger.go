package exec

import (
	"cmp"

	insaneJSON "github.com/ozontech/insane-json"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

type Merger struct {
	left, right query.RecordProducer

	curLeft, curRight *query.Record

	colIdx   int
	field    string
	dataType query.DataType
	order    seq.DocsOrder
	cmp      func(any, any) int

	// dedup drops records whose sort key repeats the previously emitted one.
	// It is enabled only for the seq.ID merge: shards may match the same
	// document, and the merged document stream must contain each seq.ID once.
	dedup   bool
	lastVal any
	// dups counts records dropped by dedup so Finalize can subtract it from the merged total.
	dups uint64

	// roots holds records whose colIdx val has been decoded (Spawn'd an
	// insaneJSON root) while comparing during the merge. They leave the merger
	// once chosen, so this is the last owner and Finalize releases them. On the
	// documents path the merger compares by SeqID and the vals stay undecoded,
	// so roots stays empty; the DataTypeDocument path is handled for
	// correctness.
	roots []*query.Record

	done bool
}

func NewMerger(
	left query.RecordProducer,
	right query.RecordProducer,
	colIdx int,
	field string,
	dataType query.DataType,
	order seq.DocsOrder,
) *Merger {
	cmpFunc := createCmpFunc()

	return &Merger{
		left:     left,
		right:    right,
		colIdx:   colIdx,
		field:    field,
		dataType: dataType,
		order:    order,
		cmp:      cmpFunc,
		dedup:    dataType == query.DataTypeSeqID,
		curLeft:  nil,
		curRight: nil,
		done:     false,
	}
}

func (m *Merger) Next() *query.Record {
	if m.done {
		return nil
	}

	for {
		r := m.mergeNext()
		if r == nil {
			return nil
		}
		if !m.dedup {
			return r
		}
		val := m.extractValue(r)
		if m.lastVal != nil && m.cmp(val, m.lastVal) == 0 {
			// Skip duplicate.
			m.dups++
			continue
		}
		m.lastVal = val
		return r
	}
}

func (m *Merger) mergeNext() *query.Record {
	if m.curLeft == nil {
		m.curLeft = m.left.Next()
	}
	if m.curRight == nil {
		m.curRight = m.right.Next()
	}

	if m.curLeft == nil && m.curRight == nil {
		m.done = true
		return nil
	}

	if m.curLeft == nil {
		r := m.curRight
		m.curRight = m.right.Next()
		return r
	}

	if m.curRight == nil {
		r := m.curLeft
		m.curLeft = m.left.Next()
		return r
	}

	leftVal := m.extractValue(m.curLeft)
	rightVal := m.extractValue(m.curRight)

	compared := m.cmp(leftVal, rightVal)
	chooseLeft := compared <= 0
	if m.order == seq.DocsOrderDesc {
		chooseLeft = compared >= 0
	}

	if chooseLeft {
		r := m.curLeft
		m.curLeft = m.left.Next()
		m.trackRoot(r)
		return r
	}

	r := m.curRight
	m.curRight = m.right.Next()
	m.trackRoot(r)
	return r
}

func (m *Merger) Finalize() *query.Summary {
	for _, r := range m.roots {
		r.Release()
	}
	// The lookahead cursors may still hold partially consumed records whose
	// colIdx val extractValue has decoded.
	if m.dataType == query.DataTypeDocument {
		if m.curLeft != nil {
			m.curLeft.Release()
		}
		if m.curRight != nil {
			m.curRight.Release()
		}
	}

	left := m.left.Finalize()
	right := m.right.Finalize()
	summary := combineSummaries(left, right)
	if m.dedup && m.dups > 0 && summary.Total >= m.dups {
		summary.Total -= m.dups
	}
	return summary
}

// trackRoot records a record leaving the merger if its colIdx val may have been
// decoded by extractValue, so Finalize can release the spawned insaneJSON root.
// Non-document types never decode an insaneJSON root, so tracking them is
// unnecessary (but harmless — Record.Release is a no-op for them).
func (m *Merger) trackRoot(r *query.Record) {
	if m.dataType == query.DataTypeDocument {
		m.roots = append(m.roots, r)
	}
}

// combineSummaries merges the final summaries of two merged branches. The
// totals are summed; an error from either side (if any) takes precedence.
func combineSummaries(left, right *query.Summary) *query.Summary {
	var total uint64
	if left != nil {
		total += left.Total
	}
	if right != nil {
		total += right.Total
	}
	summary := &query.Summary{Total: total}
	if left != nil && left.Err != nil {
		summary.Err = left.Err
	} else if right != nil && right.Err != nil {
		summary.Err = right.Err
	}
	return summary
}

func (m *Merger) extractValue(r *query.Record) any {
	val := r.Vals[m.colIdx]
	decoded := val.Decoded()

	switch m.dataType {
	case query.DataTypeSeqID:
		return decoded.(seq.ID)
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

func createCmpFunc() func(any, any) int {
	return func(a, b any) int {
		switch v := a.(type) {
		case seq.ID:
			w := b.(seq.ID)
			switch {
			case seq.Less(v, w):
				return -1
			case seq.Less(w, v):
				return 1
			default:
				return 0
			}
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
			return 0
		}
	}
}

func NewNMergedProducers(
	producers []query.RecordProducer,
	colIdx int,
	field string,
	dataType query.DataType,
	order seq.DocsOrder,
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

func (e *emptyRecordProducer) Next() *query.Record {
	return nil
}

func (e *emptyRecordProducer) Finalize() *query.Summary {
	return nil
}
