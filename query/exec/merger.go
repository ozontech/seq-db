package exec

import (
	"fmt"

	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/seq"
)

type Merger[T any] struct {
	left, right query.RecordProducer

	curLeft, curRight *query.Record

	col   query.Column[T]
	order seq.DocsOrder
	cmp   func(T, T) int

	// dedup drops records whose sort key repeats the previously emitted one.
	// It is enabled only for the seq.ID merge: shards may match the same
	// document, and the merged document stream must contain each seq.ID once.
	dedup   bool
	hasLast bool
	lastVal T
	// dups counts records dropped by dedup so Finalize can subtract it from the merged total.
	dups uint64

	done bool
}

func NewMerger[T any](
	left query.RecordProducer,
	right query.RecordProducer,
	col query.Column[T],
	order seq.DocsOrder,
) *Merger[T] {
	if col.Cmp() == nil {
		panic(fmt.Sprintf("BUG: %s column cannot be merged", col.DataType()))
	}

	return &Merger[T]{
		left:     left,
		right:    right,
		col:      col,
		order:    order,
		cmp:      col.Cmp(),
		dedup:    col.DataType() == query.DataTypeSeqID,
		curLeft:  nil,
		curRight: nil,
		done:     false,
	}
}

func (m *Merger[T]) Next() *query.Record {
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
		val := m.col.Val(r)
		if m.hasLast && m.cmp(val, m.lastVal) == 0 {
			// Skip duplicate.
			m.dups++
			continue
		}
		m.lastVal = val
		m.hasLast = true
		return r
	}
}

func (m *Merger[T]) mergeNext() *query.Record {
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

	leftVal := m.col.Val(m.curLeft)
	rightVal := m.col.Val(m.curRight)

	compared := m.cmp(leftVal, rightVal)
	chooseLeft := compared <= 0
	if m.order == seq.DocsOrderDesc {
		chooseLeft = compared >= 0
	}

	if chooseLeft {
		r := m.curLeft
		m.curLeft = m.left.Next()
		return r
	}

	r := m.curRight
	m.curRight = m.right.Next()
	return r
}

func (m *Merger[T]) Finalize() *query.Summary {
	left := m.left.Finalize()
	right := m.right.Finalize()
	summary := combineSummaries(left, right)
	if m.dedup && m.dups > 0 && summary.Total >= m.dups {
		summary.Total -= m.dups
	}
	return summary
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

func NewNMergedProducers[T any](
	producers []query.RecordProducer,
	col query.Column[T],
	order seq.DocsOrder,
) query.RecordProducer {
	l := len(producers)
	if l == 0 {
		return &emptyRecordProducer{}
	}
	if l == 1 {
		return NewMerger(producers[0], &emptyRecordProducer{}, col, order)
	}
	if l == 2 {
		return NewMerger(producers[0], producers[1], col, order)
	}

	half := l / 2
	a := NewNMergedProducers(producers[:half], col, order)
	b := NewNMergedProducers(producers[half:], col, order)

	return NewMerger(a, b, col, order)
}

type emptyRecordProducer struct{}

func (e *emptyRecordProducer) Next() *query.Record {
	return nil
}

func (e *emptyRecordProducer) Finalize() *query.Summary {
	return nil
}
