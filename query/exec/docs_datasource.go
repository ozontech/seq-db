package exec

import (
	"context"
	"errors"

	"github.com/ozontech/seq-db/frac/processor"
	"github.com/ozontech/seq-db/fracmanager"
	"github.com/ozontech/seq-db/query"
	"github.com/ozontech/seq-db/querytracer"
	"github.com/ozontech/seq-db/seq"
)

const searcherBatchLimit = 100

// DocsDataSource is limitless: it walks the matched set in fixed-size batches
// via cursor pagination:
//   - repeatedly call Searcher.SearchDocs with a constant per-batch limit and a
//     cursor (OffsetId) advanced from the previous batch's last ID;
//   - the Fetcher.FetchDocs step is performed on demand — only the current
//     document is fetched right before it is returned from Next().
type DocsDataSource struct {
	ctx context.Context
	tr  *querytracer.Tracer

	searchParams processor.SearchParams

	fracManager *fracmanager.FracManager
	searcher    *fracmanager.Searcher
	fetcher     *fracmanager.Fetcher

	qpr    *seq.QPR
	curIdx int

	// total accumulates the final total.
	total uint64
	// batchNo counts completed batches; 0 means no batch has been scanned yet.
	batchNo int
	// done marks the documents path as exhausted (a batch returned no IDs).
	done bool

	// fracs are acquired once and held for the producer's lifetime so that
	// cursor-pagination batches and per-document fetches share the same set.
	fracs   fracmanager.List
	release func()

	// err holds the first error encountered during scanning/fetching. Reported via Finalize.
	err error
}

func NewDocsDataSource(
	ctx context.Context,
	tr *querytracer.Tracer,
	searchParams processor.SearchParams,
	fracManager *fracmanager.FracManager,
	searcher *fracmanager.Searcher,
	fetcher *fracmanager.Fetcher,
) *DocsDataSource {
	searchParams.AggQ = nil
	if ctx == nil {
		ctx = context.Background()
	}

	return &DocsDataSource{
		ctx:          ctx,
		tr:           tr,
		searchParams: searchParams,
		fracManager:  fracManager,
		searcher:     searcher,
		fetcher:      fetcher,
	}
}

func (s *DocsDataSource) Next() *query.Record {
	return s.nextDoc()
}

func (s *DocsDataSource) Finalize() *query.Summary {
	if s.release != nil {
		s.release()
	}
	return &query.Summary{Total: s.total, Err: s.err}
}

func (s *DocsDataSource) nextDoc() *query.Record {
	if s.done {
		return nil
	}

	if s.qpr == nil || s.curIdx >= len(s.qpr.IDs) {
		if err := s.scanBatch(); err != nil {
			s.err = err
			return nil
		}
		if s.done {
			return nil
		}
	}

	idSrc := s.qpr.IDs[s.curIdx]
	s.curIdx++

	docs, err := s.fetcher.FetchDocs(s.ctx, s.fracs, []seq.IDSource{idSrc}, true)
	if err != nil {
		s.err = err
		return nil
	}

	return makeDocumentRecord(idSrc.ID, docs[0])
}

// scanBatch runs one SearchDocs iteration of the documents path with cursor
// pagination. The first batch honors searchParams.WithTotal (to capture the
// full total once); subsequent batches drop WithTotal and advance the cursor
// (OffsetId) from the previous batch's last ID, narrowing the time range.
func (s *DocsDataSource) scanBatch() error {
	params := s.searchParams

	if s.fracs == nil {
		s.fracs, s.release = s.fracManager.AcquireFractionsInRange(params.From, params.To)
	}

	if params.Limit == 0 || params.Limit > searcherBatchLimit {
		params.Limit = searcherBatchLimit
	}

	if s.batchNo > 0 {
		// Advance the cursor
		params.WithTotal = false
		lastID := s.qpr.IDs[len(s.qpr.IDs)-1].ID
		params.OffsetId = lastID
	}

	qpr, err := s.searcher.SearchDocs(s.ctx, s.fracs, params, s.tr)
	if err != nil {
		return err
	}
	if err := qprErrors(qpr); err != nil {
		return err
	}

	if len(qpr.IDs) == 0 {
		s.done = true
		return nil
	}

	s.qpr = qpr
	s.curIdx = 0
	s.batchNo++
	if params.WithTotal {
		s.total = qpr.Total
	}
	return nil
}

// qprErrors joins all store-level errors reported in the QPR, if any.
func qprErrors(qpr *seq.QPR) error {
	if len(qpr.Errors) == 0 {
		return nil
	}
	var resErr error
	for _, e := range qpr.Errors {
		resErr = errors.Join(errors.New(e.ErrStr))
	}
	return resErr
}

func makeDocumentRecord(id seq.ID, payload []byte) *query.Record {
	return &query.Record{
		Vals: []*query.RecordVals{
			query.NewSeqIDVals(id),
			query.NewDocVals(payload),
		},
	}
}
