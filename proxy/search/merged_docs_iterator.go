package search

import (
	"context"
	"errors"
	"io"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type mergedDocStream struct {
	a, b       DocsIterator
	less       func(seq.IDSource, seq.IDSource) bool
	docA, docB StreamingDoc
	init       bool
}

func newMergedDocsStream(
	a, b DocsIterator,
	less func(seq.IDSource, seq.IDSource) bool,
) *mergedDocStream {
	m := &mergedDocStream{
		a:    a,
		b:    b,
		less: less,
	}
	return m
}

func (m *mergedDocStream) readA() StreamingDoc {
	var err error
	current := m.docA
	if m.docA, err = m.a.Next(); err != nil {
		m.a = nil // stop reading A
		if !errors.Is(err, io.EOF) {
			metric.FetchErrors.Inc()
			logger.Error("fetch error",
				zap.Error(err),
				zap.String("doc_id", m.docB.ID.String()))
		}
	}
	return current
}

func (m *mergedDocStream) readB() StreamingDoc {
	var err error
	current := m.docB
	if m.docB, err = m.b.Next(); err != nil {
		m.b = nil // stop reading B
		if !errors.Is(err, io.EOF) {
			metric.FetchErrors.Inc()
			logger.Error("fetch error",
				zap.Error(err),
				zap.String("doc_id", m.docB.ID.String()))
		}
	}
	return current
}

func (m *mergedDocStream) Next() (StreamingDoc, error) {
	if !m.init {
		m.readA()
		m.readB()
		m.init = true
	}

	if m.a == nil && m.b == nil {
		return StreamingDoc{}, io.EOF
	}
	if m.a == nil {
		return m.readB(), nil
	}
	if m.b == nil {
		return m.readA(), nil
	}

	if m.less(m.docB.IDSource(), m.docA.IDSource()) {
		return m.readB(), nil
	}
	return m.readA(), nil
}

func newNMergedStreams(streams []DocsIterator, less func(seq.IDSource, seq.IDSource) bool) DocsIterator {
	if len(streams) == 0 {
		return EmptyDocsStream{}
	}

	if len(streams) == 1 {
		return newMergedDocsStream(streams[0], EmptyDocsStream{}, less)
	}

	merged := newMergedDocsStream(streams[0], streams[1], less)
	for _, s := range streams[2:] {
		merged = newMergedDocsStream(merged, s, less)
	}
	return merged
}

type mergedStreamIterator struct {
	docsFetched  int
	idsProcessed int
	idsTotal     int
	stream       DocsIterator
	nextDoc      StreamingDoc
	nextErr      error
	less         func(seq.IDSource, seq.IDSource) bool
	ids          []seq.IDSource
	ctx          context.Context
}

func newMergedStreamIterator(ctx context.Context, streams []DocsIterator, ids []seq.IDSource) *mergedStreamIterator {
	less := lessFuncPosBased(ids)
	mergedStream := newNMergedStreams(streams, less)

	m := &mergedStreamIterator{
		idsTotal: len(ids),
		stream:   mergedStream,

		ctx:  ctx,
		less: less,
		ids:  ids,
	}
	m.loadNextDoc()
	return m
}

func (m *mergedStreamIterator) loadNextDoc() {
	nextDoc, nextErr := m.stream.Next()
	if !errors.Is(nextErr, io.EOF) {
		m.docsFetched++
	}
	m.nextDoc, m.nextErr = nextDoc, nextErr
}

func (m *mergedStreamIterator) Next() (StreamingDoc, error) {
	if len(m.ids) == 0 || util.IsCancelled(m.ctx) {
		metric.DocumentsFetched.Observe(float64(m.docsFetched))
		return StreamingDoc{}, io.EOF
	}

	m.idsProcessed++
	currentID := m.ids[0]
	m.ids = m.ids[1:]
	for ; m.nextErr == nil && m.less(m.nextDoc.IDSource(), currentID); m.loadNextDoc() { // fast-forward
		logger.Error("unexpected doc received",
			zap.Int("ids_total", m.idsTotal),
			zap.Int("ids_processed", m.idsProcessed),
			zap.Int("docs_fetched", m.docsFetched),
			zap.String("doc_id", m.nextDoc.ID.String()),
			zap.String("current_id", currentID.ID.String()),
		)
	}
	if m.nextErr != nil || !currentID.Equal(m.nextDoc.IDSource()) {
		// either there are no more doc, or the doc ID is greater than we are expecting
		return NewStreamingDoc(currentID, nil), nil // not found
	}
	curDoc := m.nextDoc
	m.loadNextDoc()
	return curDoc, nil
}
