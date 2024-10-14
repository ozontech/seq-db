package search

import (
	"errors"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"

	"github.com/ozontech/seq-db/logger"
	"github.com/ozontech/seq-db/metric"
	"github.com/ozontech/seq-db/pkg/storeapi"
	"github.com/ozontech/seq-db/seq"
	"github.com/ozontech/seq-db/util"
)

type DocsIterator interface {
	Next() (StreamingDoc, error)
}

type EmptyDocsStream struct {
}

func (e EmptyDocsStream) Next() (StreamingDoc, error) {
	return StreamingDoc{}, io.EOF
}

// ReadAll is currently used only in tests
func ReadAll(i DocsIterator) (dst [][]byte) {
	for doc, err := i.Next(); err == nil; doc, err = i.Next() {
		dst = append(dst, doc.Data)
	}
	return
}

type uniqueIDIterator struct {
	it         DocsIterator
	last       StreamingDoc
	err        error
	duplicates int
}

// For correct operation it is necessary that the docs in the stream be grouped by ID
func newUniqueIDIterator(it DocsIterator) *uniqueIDIterator {
	uniqueIt := &uniqueIDIterator{
		it: it,
	}
	uniqueIt.last, uniqueIt.err = it.Next()
	return uniqueIt
}

func (u *uniqueIDIterator) Next() (StreamingDoc, error) {
	found := u.last
	notEmpty := 0

	if u.err != nil {
		metric.FetchDuplicateErrors.Observe(float64(u.duplicates))
		return StreamingDoc{}, u.err
	}

	for {
		if !u.last.Empty() {
			notEmpty++
			found = u.last
		}
		prev := u.last
		if u.last, u.err = u.it.Next(); u.err != nil || !u.last.ID.Equal(prev.ID) {
			u.duplicates += max(0, notEmpty-1)
			return found, nil
		}
	}

	// nolint
	panic("unreachable")
}

type grpcStreamIterator struct {
	source   uint64
	host     string
	stream   storeapi.StoreApi_FetchClient
	totalIDs int

	fetched int
}

func newGrpcStreamIterator(stream storeapi.StoreApi_FetchClient, host string, source uint64, totalIDs int) *grpcStreamIterator {
	return &grpcStreamIterator{
		stream:   stream,
		source:   source,
		host:     host,
		totalIDs: totalIDs,
	}
}

func (s *grpcStreamIterator) Next() (StreamingDoc, error) {
	data, err := s.stream.Recv()
	if errors.Is(err, io.EOF) && s.fetched != s.totalIDs {
		metric.FetchNotFoundError.Observe(float64(s.totalIDs - s.fetched))
		return StreamingDoc{}, fmt.Errorf("wrong fetched doc count=%d, requested=%d", s.fetched, s.totalIDs)
	}

	if errors.Is(err, io.EOF) {
		return StreamingDoc{}, io.EOF
	}

	if err != nil {
		return StreamingDoc{Source: s.source}, err
	}

	doc := unpackDoc(data.Data, s.source)
	if !doc.Empty() {
		s.fetched++
	} else {
		logger.Warn("doc hasn't been fetched",
			zap.String("doc_id", doc.ID.String()),
			zap.String("store", s.host))
	}
	return doc, nil
}

type explainWrapperIterator struct {
	wrapped DocsIterator

	fetched  int
	received int

	ids       []seq.IDSource
	host      string
	startTime time.Time
}

func newExplainWrapperIterator(it DocsIterator, ids []seq.IDSource, host string, startTime time.Time) *explainWrapperIterator {
	return &explainWrapperIterator{
		wrapped:   it,
		ids:       ids,
		host:      host,
		startTime: startTime,
	}
}

func (e *explainWrapperIterator) Next() (StreamingDoc, error) {
	e.fetched++
	d, err := e.wrapped.Next()
	if errors.Is(err, io.EOF) {
		logger.Info("fetch stats",
			zap.Int("ids_total", len(e.ids)),
			zap.Int("received", e.received),
			zap.Int("fetched", e.fetched),
			zap.String("store", e.host),
			util.ZapDurationWithPrec("took_ms", time.Since(e.startTime), "ms", 2),
		)

		return StreamingDoc{}, io.EOF
	}

	if err != nil {
		return StreamingDoc{}, err
	}
	if !d.Empty() {
		e.received++
	}
	return d, nil
}
